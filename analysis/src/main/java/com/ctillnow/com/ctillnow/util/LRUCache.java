package com.ctillnow.com.ctillnow.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/31 19:06
 * 4
 */
public class LRUCache extends LinkedHashMap<String, Integer> {
    private static final long serialVersionUID = 1L;
    protected int maxElements;

    public LRUCache(int maxSize) {
        super(maxSize, 0.75F, true);
        this.maxElements = maxSize;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.LinkedHashMap#removeEldestEntry(java.util.Map.Entry)
     */
    @Override
    protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
        return (size() > this.maxElements);
    }
}
