package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    //定义代理对象，必须由主线程获取
    private IVoucherOrderService proxy;
    //配置lua脚本信息
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static{
        SECKILL_SCRIPT=new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("Seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
//    //阻塞队列
//    private BlockingQueue<VoucherOrder> orderTasks=new ArrayBlockingQueue<>(1024*1024);
    //创建线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR= Executors.newSingleThreadExecutor();

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //判断秒杀是否开始
//        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            //尚未开始
//            return Result.fail("秒杀尚未开始");
//        }
//        //判断秒杀是否已经结束
//        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
//            //已经结束
//            return Result.fail("秒杀已经结束");
//        }
//        //判断库存是否充足
//        if(voucher.getStock()<1){
//            //库存不足
//            return Result.fail("库存不足");
//        }
//        Long userId = UserHolder.getUser().getId();
//        log.info("userId:{}",userId);
//        //创建锁对象
////        SimpleRedisLock lock=new SimpleRedisLock(stringRedisTemplate,"order:"+userId);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        //获取锁
//        boolean isLock = lock.tryLock();
//        //判断获取锁是否成功
//        if(!isLock){
//            //获取锁失败，返回错误或重试
//            return Result.fail("不允许重复下单");
//        }
//        try{
//            //必须要拿到当前对象的代理对象，事务才能生效
//            IVoucherOrderService proxy=(IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }finally {
//            //释放锁
//            lock.unlock();
//        }
//    }

    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    /**
     * 异步生成订单
     */
    private class VoucherOrderHandler implements Runnable{
        String queueName="stream.orders";
        @Override
        public void run() {
            while(true){
                try {
                    //1.获取消息队列中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //判断消息是否获取成功
                    if(list==null||list.isEmpty()){
                        //获取失败，说明没有消息，进入下一次循环
                        continue;
                    }
                    //解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder= BeanUtil.fillBeanWithMap(values,new VoucherOrder(),true);
                    //如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    //ack确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while(true){
                try {
                    //1.获取消息队列中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //判断消息是否获取成功
                    if(list==null||list.isEmpty()){
                        //获取失败，说明PendingList没有异常消息，结束循环
                        break;
                    }
                    //解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder= BeanUtil.fillBeanWithMap(values,new VoucherOrder(),true);
                    //如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    //ack确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理pendingList异常",e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
        private void handleVoucherOrder(VoucherOrder voucherOrder) {
            //获取用户
            Long userId = voucherOrder.getUserId();
            RLock lock = redissonClient.getLock("lock:order:" + userId);
            //获取锁
            boolean isLock = lock.tryLock();
            //判断获取锁是否成功
            if(!isLock){
                //获取锁失败
                log.error("不允许重复下单");
            }
            try{
                 proxy.createVoucherOrder(voucherOrder);
            }finally {
                //释放锁
                lock.unlock();
            }
        }
    }


//    @Override
//    public Result seckillVoucher(Long voucherId){
//        Long userId = UserHolder.getUser().getId();
//        //执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//        //判断结果是否为0
//        int r=result.intValue();
//        if(r!=0){
//            //不为0，代表没有资格购买
//            return Result.fail(r==1?"库存不足":"不能重复下单");
//        }
//        //为0，有资格购买,把下单信息保存在阻塞队列
//        VoucherOrder voucherOrder=new VoucherOrder();
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        voucherOrder.setUserId(userId);
//        voucherOrder.setVoucherId(voucherId);
//        orderTasks.add(voucherOrder);
//
//        //获取代理对象
//       proxy=(IVoucherOrderService) AopContext.currentProxy();
//        //返回订单id
//        return Result.ok(orderId);
//    }

    /**
     * 使用redis进行库存和购买资格判断
     * @param voucherId
     * @return
     */
    @Override
    public Result seckillVoucher(Long voucherId){
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        //执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),String.valueOf(orderId)
        );
        //判断结果是否为0
        int r=result.intValue();
        if(r!=0){
            //不为0，代表没有资格购买
            return Result.fail(r==1?"库存不足":"不能重复下单");
        }
        //获取代理对象
        proxy=(IVoucherOrderService) AopContext.currentProxy();
        //返回订单id
        return Result.ok(orderId);
    }

    /**
     * 保存订单，扣减库存
     * @param voucherOrder
     */
    @Transactional
    public  void createVoucherOrder(VoucherOrder voucherOrder) {
        //一人一单
        Long userId=UserHolder.getUser().getId();
        //查询订单
        long count=query().eq("user_id",userId).eq("voucher_id",voucherOrder.getVoucherId()).count();
        //判断是否存在
        if(count>0){
            //用户已经购买过了
            log.error("用户已经买过一次");
        }
        //扣减库存
        boolean success=seckillVoucherService.update()
                .setSql("stock=stock-1")
                .eq("voucher_id",voucherOrder)
                .gt("stock",0)  //where id=? and stock >0
                .update();
        if(!success){
            log.error("库存不足");
        }
        save(voucherOrder);
    }
}