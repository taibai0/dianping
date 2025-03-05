package com.hmdp.service.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

import static com.hmdp.utils.RedisConstants.SHOP_LIST_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Autowired
    private StringRedisTemplate redisTemplate;

    @Override
    public Result queryTypeList() {
        List<String> shopTypeList=redisTemplate.opsForList().range(SHOP_LIST_KEY,0,-1);
        if(CollectionUtil.isNotEmpty(shopTypeList)){
            List<ShopType> types= JSONUtil.toList(shopTypeList.get(0),ShopType.class);
            return Result.ok(types);
        }
        List<ShopType> typeList=query().orderByAsc("sort").list();
        if(CollectionUtil.isEmpty(typeList)){
            return Result.fail("列表信息不存在");
        }
        String jsonStr=JSONUtil.toJsonStr(typeList);
        redisTemplate.opsForList().leftPushAll(SHOP_LIST_KEY,jsonStr);
        return Result.ok(typeList);
    }
}
