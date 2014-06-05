package com.allyes.hive.udf;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

 

public class Top4GroupBy extends UDAF {

    //定义一个对象用于存储数据
    public static class State {
        private Map<Text, IntWritable> counts;
        private int limit;

    }

    /**
     * 累加数据，判断map的key中是否存在该字符串，如果存在累加，不存在放入map中
     * @param s
     * @param o
     * @param i
     */
    private static void increment(State s, Text o, int i) {
        if (s.counts == null) {
            s.counts = new HashMap<Text, IntWritable>();
        }
        IntWritable count = s.counts.get(o);
        if (count == null) {
            Text key = new Text();
            key.set(o);
            s.counts.put(key, new IntWritable(i));
        } else {
            count.set(count.get() + i);
        }

    }

    public static class Top4GroupByEvaluator implements UDAFEvaluator {

        private final State state;

        public Top4GroupByEvaluator() {
            state = new State();
        }

        
        public void init() {
            if (state.counts != null) {
                state.counts.clear();
            }
            if (state.limit == 0) {
                state.limit = 100;
            }
        }

        public boolean iterate(Text value, IntWritable  cnt, IntWritable limits) {
            if (value == null || limits == null || cnt==null) {
                return false;
            } else {
                state.limit = limits.get();
                increment(state, value, cnt.get());
            }
            return true;
        }

        public State terminatePartial() {
            return state;
        }

        public boolean merge(State other) {
            if (state == null || other == null) {
                return false;
            }
            state.limit = other.limit;
            for (Map.Entry<Text, IntWritable> e : other.counts.entrySet()) {
                increment(state, e.getKey(), e.getValue().get());
            }
            return true;
        }

        public Text terminate() {
            if (state == null || state.counts.size() == 0) {
                return null;
            }
            Map<Text, IntWritable> it = sortByValue(state.counts, true);
            StringBuffer str = new StringBuffer();
            int i = 0;
            for (Map.Entry<Text, IntWritable> e : it.entrySet()) {
                ++i;
                if (i > state.limit) {//只输出传入条数的结果，并拼成字符串
                    break;
                }
                str.append(e.getKey().toString()).append(":").append(e.getValue().get()).append("|");
            }
            return new Text(str.toString());
        }

        /*
         * 实现一个map按值的排序算法
         */
        @SuppressWarnings("unchecked")
        public static Map sortByValue(Map map, final boolean reverse) {
            List list = new LinkedList(map.entrySet());
             
            Collections.sort(list, new Comparator() {
                public int compare(Object o1, Object o2) {
                    if (reverse) {
                        return -((Comparable) ((Map.Entry) o1).getValue()).compareTo(((Map.Entry) o2).getValue());
                    }
                    return ((Comparable) ((Map.Entry) o1).getValue()).compareTo(((Map.Entry) o2).getValue());
                }
            });

            Map result = new LinkedHashMap();
            for (Iterator it = list.iterator(); it.hasNext();) {
                Map.Entry entry = (Map.Entry) it.next();
                result.put(entry.getKey(), entry.getValue());
            }
            return result;
        }

    }

}