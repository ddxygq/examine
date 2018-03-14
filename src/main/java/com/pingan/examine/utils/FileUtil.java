package com.pingan.examine.utils;
import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import jodd.util.StringUtil;

/**
 * Created by maple leaves on 2017/8/1.
 */
public class FileUtil {
    /**
     * 获取某个文件夹下的文件数量，仅包含文件夹，不包含子文件夹
     *
     * @param path 文件的完整绝对路径
     * @return 文件夹中的文件数量
     */
    public static int getFileNumber(String path) {
        if (!isFolder(path)) {
            return 0;
        }
        File file = new File(path);
        File[] files = file.listFiles();
        if (files == null) {
            return 0;
        }
        int returnValue = 0;
        for (File tempFile : files) {
            if (tempFile.isFile()) {
                returnValue++;
            }
        }
        return returnValue;
    }
    /**
     * 删除一个文件夹
     * @param path 文件夹的完整绝对路径
     * @return 删除成功后返回true, 失败返回false
     */


    public static boolean delFolder(String path) {
        if (!isFolder(path)) {
            return false;
        }
        File file = new File(path);
        return file.delete();
    }
    /**
     * 删除一个文件
     * @param path 文件的完整绝对路径
     * @return 删除成功后返回true, 失败返回false
     */
    public static boolean delFile(String path) {
        if (!isFile(path)) {
            return false;
        }
        File file = new File(path);
        return file.delete();
    }
    /**
     * 删除文件夹下所有文件和子文件夹，但是文件夹本身不会删除
     * @param path 文件的完整绝对路径
     * @return 完全清空后返回true, 失败返回false
     */
    public static boolean delAllFile(String path) {
        if (!isFolder(path)) {
            return false;
        }
        File file = new File(path);
        File[] files = file.listFiles();
        if (file == null) {
            return false;
        }
        for (File tempFile : files) {
            if (tempFile.isFile()) {
                tempFile.delete();
            } else if (tempFile.listFiles() == null) {
                tempFile.delete();
            } else {
                delAllFile(tempFile.getPath());
                tempFile.delete();
            }
        }
        return true;
    }
    /**
     * 获取文件大小 根据传入参数决定
     * @param units 单位KB,MB,GB
     * @param path 文件的完整绝对路径
     * @return 文件的尺寸
     */
    public static double getFileSize(String path, String units) {
        if (!isFolder(path)) {
            return 0;
        }
        File file = new File(path);
        double length = file.length();
        if (units == null) {
            return length;
        } else if ("KB".equals(units.toLowerCase())) {
            return length / 1024;
        } else if ("MB".equals(units.toLowerCase())) {
            return length / 1024 / 1024;
        } else if ("GB".equals(units.toLowerCase())) {
            return length / 1024 / 1024 / 1024;
        } else {
            return length;
        }
    }
    /**
     * 把文件夹的路径和文件名组合为完整的文件路径
     * @param path 文件的完整绝对路径，不含文件名
     * @param fileName 文件名，含后缀
     * @return 组合后的完整路径
     */
    public static String getFullPath(String path, String fileName) {
        if (path == null || path.trim().equals("")
                || fileName == null || fileName.trim().equals("")) {
            return null;
        } else if (path.endsWith("\\") || path.endsWith("/")) {
            return path + fileName;
        } else {
            return path + File.separator + fileName;
        }
    }
    /**
     * 检查文件夹是是都不为为空
     * @param paths 文件夹路径集合
     * @return 都不为空ture，否则false
     */
    public static boolean isNotNullForPaths(List<String> paths) {
        if (paths == null) {
            return false;
        }
        for (String path : paths) {
            if (!isFolder(path)) {
                return false;
            }
            File tempFile = new File(path);
            if (tempFile.list() == null) {
                return false;
            }

        }
        return true;
    }
    /**
     * 判断传入路径是否是文件夹
     * @param path 文件的完整绝对路径
     * @return 路径非法，路径表示文件不存在或不是文件夹均返回false,否则换回ture
     */
    public static boolean isFolder(String path) {
        if (path == null || path.trim().equals("")) {
            return false;
        }
        File file = new File(path);
        if ((!file.exists()) || file.isFile()) {
            return false;
        }
        return true;
    }
    /**
     * 判断传入路径是否是文件
     *
     * @param path 文件的完整绝对路径
     * @return 路径非法，路径表示文件不存在或不是文件夹均返回false,否则换回ture
     */
    public static boolean isFile(String path) {
        if (path == null || path.trim().equals("")) {
            return false;
        }
        File file = new File(path);
        if ((!file.exists()) || file.isDirectory()) {
            return false;
        }
        return true;
    }
    /**
     * 保存文件到指定路径，如果该文件不存在则创建
     * @param data 文件的内容
     * @param path 文件的完整绝对路径
     * @return 保存成功返回true，否则false
     * @throws IOException
     */
    public static boolean writeByteToFile(String path, byte[] data) throws IOException {
        if (data == null || !checkFile(path, new String(data))) {
            return false;
        }
        if (path == null || path.trim().equals("")) {
            return false;
        }
        OutputStream out = new FileOutputStream(path);
        OutputStream outBuffer = new BufferedOutputStream(out, data.length);
        outBuffer.write(data);
        outBuffer.flush();
        outBuffer.close();
        return true;
    }
    /**
     * 保存文件到指定文件  如果该文件不存在则创建   传入String
     * @param path        文件的完整绝对路径
     * @param fileContent 要保存的内容
     * @param flag        如果为true 则向现有文件中添加，否则清空并新写入
     * @return 保存成功返回true ，否则false
     * @throws IOException
     */
    public static boolean writeFile(String path, String fileContent, boolean flag) throws IOException {
        if (!checkFile(path, fileContent)) {
            return false;
        }
        OutputStream out = new FileOutputStream(path, flag);
        Writer writer = new OutputStreamWriter(out, "utf-8");
        writer.write(fileContent + "\r\n");
        writer.flush();
        writer.close();
        return true;
    }
    /**
     * 添加内容到指定文件  如果该文件不存在则创建   传入List
     * @param path        文件的完整绝对路径
     * @param fileContent 要保存的内容集合
     * @param flag        如果为true 则向现有文件中添加，否则清空并新写入
     * @return 保存成功返回true ，否则false
     * @throws IOException
     */
    public static boolean writeFile(String path, List<String> fileContent, boolean flag) throws IOException {
        if (!checkFile(path, fileContent)) {
            return false;
        }
        OutputStream out = new FileOutputStream(path, flag);
        Writer writer = new OutputStreamWriter(out, "utf-8");
        for (String str : fileContent) {
            writer.write(str + "\r\n");
        }
        writer.flush();
        out.close();
        return true;
    }
    /**
     * 添加内容到制定文件 如果该文件不存在则创建
     * @param path        文件的绝对路径(不含文件名)
     * @param fileName    文件ming
     * @param fileContent 保存的内容集合
     * @param flag        如果为true 则向现有文件中添加，否则清空并新写入
     * @return 保存成功返回true ，否则false
     * @throws IOException
     */
    public static boolean writeFile(String path, String fileName, List<String> fileContent, boolean flag) throws IOException {
        String fullPath = getFullPath(path, fileName);
        if (fullPath != null) {
            return writeFile(fullPath, fileContent, flag);
        } else {
            return false;
        }
    }
    /**
     * 根据路径读取一个文件            path
     * 如果路径错误或不存在或空返回尺寸为0的List
     * @param path 文件的完整绝对路径
     * @return 读取到文件的内容
     * @throws IOException
     */
    public static List<String> readFile(String path) throws IOException {
        List<String> returnValue = new ArrayList<>();
        if (!isFile(path)) {
            return returnValue;
        }
        InputStream inputStream = new FileInputStream(path);
        Reader reader = new InputStreamReader(inputStream, "utf-8");
        LineNumberReader lnr = new LineNumberReader(reader);
        while (true) {
            String str = lnr.readLine();
            if (str == null) {
                break;
            }
            returnValue.add(str);
        }
        lnr.close();
        reader.close();
        inputStream.close();
        return returnValue;
    }
    /**
     * 读取文件
     * 如果路径错误或不存在或空返回尺寸为0的List
     * @param file 要读取的文件
     * @return 读取到的内容
     * @throws IOException
     */
    public static List<String> readFile(File file) throws IOException {
        List<String> returnValue = new ArrayList<>();
        if (file == null || !file.exists() || file.isDirectory()) {
            return returnValue;
        }
        InputStream inputStream = new FileInputStream(file);
        Reader reader = new InputStreamReader(inputStream, "utf-8");
        LineNumberReader lnr = new LineNumberReader(reader);
        while (true) {
            String str = lnr.readLine();
            if (str == null) {
                break;
            }
            returnValue.add(str);
        }
        inputStream.close();
        return returnValue;
    }

    /**
     * 读取一个文件，并排重后返回
     * 如果路径错误，文件不存在，为空，返回为0的set
     * @param path
     * @return 读取到的文件内容
     * @throws IOException
     */
    public static Set<String>readFileNoDup(String path) throws IOException {
        Set<String> rerurnValue = new HashSet<>();
        if (!isFile(path)) {
            return rerurnValue;
        }
        InputStream inputStream = new FileInputStream(path);
        Reader reader = new InputStreamReader(inputStream, "utf-8");
        LineNumberReader lnr = new LineNumberReader(reader);
        while (true) {
            String str = lnr.readLine();
            if (str == null) {
                break;
            }
            rerurnValue.add(str);
        }
        inputStream.close();
        return  rerurnValue;
    }

    /**
     * 读取第一个文件，排重后写入第二个文件 并把排重结果返回
     * 如果路径错误，文件不存在，为空，返回为0的set
     * @param path1
     * @param path2
     * @return
     * @throws IOException
     */
    public static List<String>excludeDuplicates(String path1,String path2) throws IOException {
        Set<String>set=readFileNoDup(path1);
        List<String>list=new ArrayList<>();
        list.addAll(set);
        return list;
    }


    /**
     * 检查文件路径是否合法且存在，不存在，则创建（递归创建目录）     context
     * @param path
     * @param context  要保存的内容
     * @return  路径非法或内容为空返回false，否则true
     * @throws IOException
     */
    public static boolean checkFile(String path, String context) throws IOException {
        if (context == null || context.trim().equals("")
                || path == null || path.trim().equals("")) {
            return false;
        }
        File file = new File(path);
        if (!isFile(path)){
        	String [] pathplus=path.split("/");
        	if (pathplus.length==1) {
        		String subpath = path.substring(0, path.lastIndexOf("\\"));
                File tempFile = new File(subpath);
                tempFile.mkdirs();
                if (!file.createNewFile()) {
                    return false;
                }
			}else {             
                String subpath = path.substring(0, path.lastIndexOf("/"));
                File tempFile = new File(subpath);
                tempFile.mkdirs();
                if (!file.createNewFile()) {
                    return false;
                }
			}     
        }
        return true;
    }
    /**
     *检查文件路径是否合法且存在，不存在，则创建（递归创建目录）    list
     * @param path
     * @param list  要保存的内容集合
     * @return
     * @throws IOException
     */
    public static boolean checkFile(String path, List<String> list) throws IOException {
        if (list == null || list.size()<1
                || path == null || path.trim().equals("")) {
            return false;
        }
        File file = new File(path);
        if (!isFile(path)) {
        	String [] pathplus=path.split("/");
        	if (pathplus.length==1) {
        		 String subpath = path.substring(0, path.lastIndexOf("/"));
                 File tempFile = new File(subpath);
                 tempFile.mkdirs();
                 if (!file.createNewFile()) {
                     return false;
                 }
			}else {
            String subpath = path.substring(0, path.lastIndexOf("\\"));
            File tempFile = new File(subpath);
            tempFile.mkdirs();
            if (!file.createNewFile()) {
                return false;
            }
			}
        }
        return true;
    }
    /**
     * 检查文件夹是否合法且存在，不存在，则创建（递归创建目录）     context
     * @param path
     * @return  路径非法或内容为空返回false，否则true
     * @throws IOException
     */
    public static boolean checkForder(String path) throws IOException {
        if ( path == null || path.trim().equals("")) {
            return false;
        }
        File file = new File(path);
        if (!isFolder(path)){
        	String [] pathplus=StringUtil.split(path, "/");
        	String subpath;
        	if (pathplus.length<=1) {
        		  subpath = path.substring(0, path.lastIndexOf("\\"));
			}else {
				  subpath = path.substring(0, path.lastIndexOf("/"));
			}         
            File tempFile = new File(subpath);
            tempFile.mkdirs();
            if (!file.createNewFile()) {
                return false;
            }
        }
        return true;
    }
}

