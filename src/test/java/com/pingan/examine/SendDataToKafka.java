package com.pingan.examine;

import com.alibaba.fastjson.JSON;
import com.pingan.examine.bean.D504Bean;
import com.pingan.examine.bean.D505Bean;
import com.pingan.examine.bean.RecordBean;
import com.pingan.examine.start.ConfigFactory;
import jodd.util.ThreadUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.io.*;
import java.util.*;

/**
 * 数据发动机
 * Created by Administrator on 2017/12/12.
 */
public class SendDataToKafka {

    public static void main(String[] args) throws IOException {
        new SendDataToKafka().sendData();
    }

    @Test
    public void sendData() throws IOException {
        ConfigFactory.initConfig();
        List<D504Bean> d504BeanList = getD504Bean();
        Map<String,List<D505Bean>> d504BeanMap = getD505Bean();
        Properties pro = new Properties();
        pro.put("bootstrap.servers",ConfigFactory.kafkaip+":"+ConfigFactory.kafkaport);
        pro.put("acks", "0");
        pro.put("retries", 0);
        pro.put("batch.size", 16384);
        pro.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pro.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(pro);
        int cnt = 0;
        while(true){
            D504Bean d504Bean = d504BeanList.get(cnt);
            RecordBean recordBean = new RecordBean(d504Bean,d504BeanMap.get(d504Bean.getD504_01()));
            String dataMesage = JSON.toJSONString(recordBean); //转化为json数据后利用Kafka进行发送
            /*ProducerRecord(topic, key, value)：主题、*/
            ProducerRecord<String,String> message = new ProducerRecord<String, String>("reimbursement-message",String.valueOf(cnt),dataMesage);
            kafkaProducer.send(message);
            System.out.println("发送消息：" + cnt + dataMesage);
            ThreadUtil.sleep(1000);
            cnt ++;
            if(cnt == d504BeanList.size()){
                cnt = 0;
            }
        }
    }

    /**
     * 获取所有的D504Bean对象
     * @return 组装好的D504Bean
     * @throws IOException
     */
    private List<D504Bean> getD504Bean() throws IOException {
        String filename = "d504.sql";
        List<List<String>> d504List = getSourceData(filename);
        List<D504Bean> returnValue = new ArrayList<>(d504List.size());
        for (List<String> oneData:d504List){
            if (oneData != null && oneData.size() == 65){
                List<String> newList = new ArrayList<>(oneData.size());
                for (String str:oneData){
                    newList.add(str.trim().replaceAll("'",""));
                }
                D504Bean bean = new D504Bean(newList);
                returnValue.add(bean);
            }
        }
        return returnValue;
    }

    /**
     * 获取所有的D505Bean对象
     * @return
     * @throws IOException
     */
    private Map<String,List<D505Bean>> getD505Bean() throws IOException {
        String filename2 = "d505.sql";
        List<List<String>> d505List = getSourceData(filename2);
        Map<String,List<D505Bean>> returnValue = new HashMap<>(d505List.size());
        for (List<String> oneData:d505List){
            if(oneData != null && oneData.size() == 44){
                List<String> newList = new ArrayList<>(oneData.size());
                for (String str:oneData){
                    newList.add(str.trim().replaceAll("'",""));
                }
                D505Bean bean = new D505Bean(newList);//实例化D505Bean对象
                if(returnValue.get(bean.getD505_01()) == null){  //第一次遇到，就新建一个值集合
                    List<D505Bean> tempList = new ArrayList<>();
                    tempList.add(bean);
                    returnValue.put(bean.getD505_01(),tempList); //用第一个属性作为键
                }else {  //已经存在，就把值取出来，继续添加
                    List<D505Bean> tempList = returnValue.get(bean.getD505_01());
                    tempList.add(bean);
                }
            }
        }
        return returnValue;
    }

    /**
     * 读取测试数据文件到内存
     * @param filename 要读取的文件
     * @return 读取到的数据文件，并按照逗号拆分
     * @throws IOException
     */
    private List<List<String>> getSourceData(String filename) throws IOException {
        //保存一个数据文件的所有行，每一行用一个String集合保存，再把这个集合放进一个集合中
        List<List<String>> returnValue = new ArrayList<>();
        String inpath = "E:\\bigData\\项目\\智能审核系统\\数据文件\\";   //读取文件目录，最好放到配置文件里面
        InputStream inputStream = new FileInputStream(inpath+filename);
        Reader reader = new InputStreamReader(inputStream,"utf8");
        LineNumberReader lnr = new LineNumberReader(reader);
        while(true){
            String str = lnr.readLine();
            if(str == null){
                break;
            }

            //处理{"to_date\\("}模式前面的
            if (StringUtils.startsWith(str,"values")){

                //保存每一行数据，将每一行字段封装成一个String集合
                List<String> list = new ArrayList<>();
                str = str.substring(8,str.length()-2);
                String[] array = str.split("to_date\\(");
                list.addAll(Arrays.asList(array[0].split(",")));
                if(list.get(list.size() - 1).equals(" ")){
                    list.remove(list.size() -1 );
                }

                //处理第二个开始的含有时间格式的数据
                for (int cnt = 1;cnt < array.length;cnt ++){
                    List<String> tempList = new ArrayList<>(Arrays.asList(array[cnt].split(",")));
                    tempList.remove(1);
                    if(tempList.get(tempList.size() - 1).equals(" ")){
                        tempList.remove(tempList.size() - 1);
                    }
                    list.addAll(tempList);
                }
                returnValue.add(list);
            }
        }
        inputStream.close();
        return returnValue;
    }
}
