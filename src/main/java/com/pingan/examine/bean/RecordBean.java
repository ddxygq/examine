package com.pingan.examine.bean;

import java.util.List;

/**
 * 完整的就诊记录信息
 * Created by Administrator on 2017/12/12.
 */
public class RecordBean {

    /**
     * 这样定义类可以使得每一个D504Bean对象与多个对应的个D505Bean对象一起绑定，进而可以将其转化为json数据利用进行Kafak进行传输
     */
    private D504Bean d504Bean;
    private List<D505Bean> d505BeanList;

    public RecordBean(D504Bean d504Bean,List<D505Bean> d505BeanList){
        this.d504Bean = d504Bean;
        this.d505BeanList = d505BeanList;
    }

    public D504Bean getD504Bean() {
        return d504Bean;
    }

    public void setD504Bean(D504Bean d504Bean) {
        this.d504Bean = d504Bean;
    }

    public List<D505Bean> getD505BeanList() {
        return d505BeanList;
    }

    public void setD505BeanList(List<D505Bean> d505BeanList) {
        this.d505BeanList = d505BeanList;
    }
}
