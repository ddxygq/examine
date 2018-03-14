package com.pingan.examine.utils;

import com.pingan.examine.start.ConfigFactory;
import java.util.TimerTask;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/12/15.
 */
public class UpdateHdfsTask extends TimerTask {
    private String adoptlocalpath = ConfigFactory.localpath + File.separator + "adopt"; //本地原始文件夹
    private String adoptlocalmargePath = ConfigFactory.localpath + File.separator + "margeadopt"; //本地合并文件夹
    private String adopthdfspath = ConfigFactory.hdfspath  + File.separator + "adopt"; //hdfs文件夹
    private String noadoptlocalpath = ConfigFactory.hdfspath + File.separator + "noadopt";
    private String noadoptlocalmargePath = ConfigFactory.localpath + File.separator + "margenoadopt";
    private String noadopthdfspath = ConfigFactory.hdfspath + File.separator;
    private int filelinenumber = 1000; //合并文件时,每个文件的行数
    @Override
    public void run(){
        System.out.println("定时器执行");
        try{
            boolean flag = margeFile(adoptlocalpath,adoptlocalmargePath);
            System.out.println("合并通过文件完成，合并结果：" + flag);
            if(flag){
                flag = HdfsUtil.localFolderUploadHDFS(ConfigFactory.hadoopconf,adoptlocalmargePath,adopthdfspath);
                System.out.println("通过文件上传hdfs完成，上传结果：" + flag);
                if(flag){
                    FileUtil.delAllFile(adoptlocalmargePath);
                }
            }
            flag = margeFile(noadoptlocalpath,noadoptlocalmargePath);
            System.out.println("合并未通过文件完成，合并结果：" + flag);
            if(flag){
                flag = HdfsUtil.localFolderUploadHDFS(ConfigFactory.hadoopconf,noadoptlocalmargePath,noadopthdfspath);
                System.out.println("未通过文件上传hdfs完成，上传结果：" + flag);
                if (flag){
                    FileUtil.delAllFile(noadoptlocalmargePath);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 合并文件并写入磁盘
     * @param localpath 原始文件保存路径
     * @param margepath 合并后的文件保存路径
     * @return 完全写成功返回true，否则返回false
     * @throws IOException
     */
    private boolean margeFile(String localpath,String margepath) throws IOException {
        File[] files = new File(localpath).listFiles();
        if(files == null || files.length < 1){
            System.out.println("本地没有要上传的文件：" + localpath);
            return false;
        }
        List<String> margeList = new ArrayList<String>(filelinenumber);
        for (File file:files){
            InputStream inputStream = new FileInputStream(file);
            Reader reader = new InputStreamReader(inputStream,"utf-8");
            LineNumberReader lnr = new LineNumberReader(reader);
            StringBuffer sb = new StringBuffer();
            int cnt = 0;
            while(true){
                String str = lnr.readLine();
                if(str == null){
                    break;
                }
                sb.append(str);
                cnt ++;
            }
            lnr.close();
            reader.close();
            inputStream.close();
            margeList.add(sb.toString());
            System.out.println(margeList.size());
            if(margeList.size() == filelinenumber && !writeFile(margeList,margepath)){
                return false;
            }
        }
        boolean flag = writeFile(margeList,margepath);
        if(flag){
            for(File file:files){
                file.delete();
            }
        }
        return flag;
    }


    /**
     * 把合并好的文件写入磁盘
     * @param magerList 合并后的文件
     * @param margepath 合并后的文件保存路径
     * @return 保存成功或没有要保存数据返回true，否则返回false
     * @throws IOException
     */
    private boolean writeFile(List<String> magerList,String margepath) throws IOException {
        if(magerList == null || magerList.size() < 1){
            return true;
        }
        String filepath = FileUtil.getFullPath(margepath,System.currentTimeMillis() + ".txt");
        boolean writeFlag = FileUtil.writeFile(filepath,magerList,true);
        magerList.clear();
        if(!writeFlag){
            System.out.println("写入文件失败：" + filepath);
            return  false;
        }else {
            return true;
        }
    }
}
