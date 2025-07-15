/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.util;

import com.huawei.bigdata.flink.examples.HttpDeleteWithBody;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * 利用HttpClient进行post请求的工具类
 *
 * @since 2020/10/10
 */
@Slf4j
public class HttpClientUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientUtil.class);

    /**
     * post请求
     *
     * @param url        url
     * @param jsonstr    requestBody json
     * @param charset    字符集
     * @param isSecurity 非安全集群可以不需要token，直接连接
     * @return 返回结果
     */
    public static String doPost(String url, String jsonstr, String charset, boolean isSecurity)
            throws Exception {
        HttpClient httpClient;
        HttpPost httpPost;
        String result = null;
        try {
            httpClient = new SSLClient();
            httpPost = new HttpPost(url);
            httpPost.addHeader("Content-Type", "application/json");
            // 关键步骤，在Cookie中加入token
            if (isSecurity) {
                httpPost.addHeader("Cookie", LoginClient.getInstance().getToken());
            }
            httpPost.setEntity(getStringEntity(jsonstr));
            HttpResponse response = httpClient.execute(httpPost);
            if (response != null) {
                HttpEntity resEntity = response.getEntity();
                if (resEntity != null) {
                    result = EntityUtils.toString(resEntity, charset);
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Http client post action error", ex);
            throw ex;
        }
        return result;
    }

    /**
     * get请求
     *
     * @param url     url
     * @param charset 字符集
     * @return 返回结果
     */
    public static String doGet(String url, String charset) throws Exception {
        HttpClient httpClient;
        HttpGet httpGet;
        String result = null;
        try {
            httpClient = new SSLClient();
            httpGet = new HttpGet(url);
            httpGet.addHeader("Content-Type", "application/json");
            httpGet.addHeader("Cookie", LoginClient.getInstance().getToken());
            HttpResponse response = httpClient.execute(httpGet);
            if (response != null) {
                HttpEntity resEntity = response.getEntity();
                if (resEntity != null) {
                    result = EntityUtils.toString(resEntity, charset);
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Http client get action error", ex);
            throw ex;
        }
        return result;
    }


    /**
     * delete请求
     *
     * @param url     url
     * @param jsonstr requestBody json
     * @param charset 字符集
     * @return 返回结果
     */
    public static String doDelete(String url, String jsonstr, String charset) throws Exception {
        HttpClient httpClient;
        HttpDeleteWithBody httpDelete;
        String result = null;
        try {
            httpClient = new SSLClient();
            httpDelete = new HttpDeleteWithBody(url);
            httpDelete.addHeader("Content-Type", "application/json");
            httpDelete.addHeader("Cookie", LoginClient.getInstance().getToken());
            if (StringUtils.isNotBlank(jsonstr)) {
                httpDelete.setEntity(getStringEntity(jsonstr));
            }
            HttpResponse response = httpClient.execute(httpDelete);
            if (response != null) {
                HttpEntity resEntity = response.getEntity();
                if (resEntity != null) {
                    result = EntityUtils.toString(resEntity, charset);
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Http client delete action error", ex);
            throw ex;
        }
        return result;
    }

    /**
     * put请求
     *
     * @param url     url
     * @param jsonstr requestBody json
     * @param charset 字符集
     * @return 返回结果
     */
    public static String doPut(String url, String jsonstr, String charset) throws Exception {
        HttpClient httpClient;
        HttpPut httpPut;
        String result = null;
        try {
            httpClient = new SSLClient();
            httpPut = new HttpPut(url);
            httpPut.addHeader("Content-Type", "application/json");
            httpPut.addHeader("Cookie", LoginClient.getInstance().getToken());
            httpPut.setEntity(getStringEntity(jsonstr));
            HttpResponse response = httpClient.execute(httpPut);
            if (response != null) {
                HttpEntity resEntity = response.getEntity();
                if (resEntity != null) {
                    result = EntityUtils.toString(resEntity, charset);
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Http client delete action error", ex);
            throw ex;
        }
        return result;
    }

    /**
     * 文件上传请求
     *
     * @param url      url
     * @param tenantId tenantId
     * @param file     字符集
     * @return 返回结果
     */
    public static String doUpload(String url, String tenantId, File file) {
        CloseableHttpClient httpClient;
        HttpPost httpPost;
        String result = null;
        CloseableHttpResponse response = null;
        try {
            httpClient = HttpClients.createDefault();
//            httpClient = new SSLClient();
            httpPost = new HttpPost(url);

            final String fileName = file.getName();
            long startTime = System.currentTimeMillis();

            // 设置请求头 boundary边界不可重复，重复会导致提交失败
            String boundary = "-------------------------" + UUID.randomUUID();
            httpPost.setHeader("Content-Type", "multipart/form-data; boundary=" + boundary);
            httpPost.addHeader("Cookie", LoginClient.getInstance().getToken());

            // 创建MultipartEntityBuilder
            MultipartEntityBuilder builder = MultipartEntityBuilder.create();
            // 设置字符编码
            builder.setCharset(StandardCharsets.UTF_8);
            // 模拟浏览器
            builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
            // 设置边界
            builder.setBoundary(boundary);
            // 设置multipart/form-data流文件
            builder.addPart("file", new FileBody(file));
            builder.addTextBody("fileType", "1");
            builder.addTextBody("tenantId", tenantId);
            // application/octet-stream代表不知道是什么格式的文件
            builder.addBinaryBody("media", file, ContentType.create("application/octet-stream"), fileName);

            HttpEntity entity = builder.build();
            httpPost.setEntity(entity);
            try {
                response = httpClient.execute(httpPost);
                if (Objects.nonNull(response) && response.getStatusLine() != null && response.getStatusLine().getStatusCode() < 400) {
                    HttpEntity he = response.getEntity();
                    if (he != null) {
                        result = EntityUtils.toString(he, "UTF-8");
                    }
                } else {
                    HttpEntity he = response.getEntity();
                    if (he != null) {
                        result = EntityUtils.toString(he, "UTF-8");
                    }
                    System.out.println("对方响应的状态码不在符合的范围内!");
                    System.out.println(result);
                    throw new RuntimeException();
                }
                return result;
            } catch (Exception e) {
                log.error("网络访问异常,请求url地址={},响应体={}", url, response, e);
                throw new RuntimeException();
            } finally {
                log.info("统一外网请求参数打印,post请求url地址={},响应={},耗时={}毫秒", url, response, (System.currentTimeMillis() - startTime));
                try {
                    if (response != null) {
                        response.close();
                    }
                    if (null != httpClient) {
                        httpClient.close();
                    }
                } catch (IOException e) {
                    log.error("请求链接释放异常", e);
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Http client upload action error", ex);
            throw ex;
        }
    }

    private static StringEntity getStringEntity(String jsonstr) throws UnsupportedEncodingException {
        StringEntity se = new StringEntity(jsonstr);
        se.setContentType("application/json");
        se.setContentEncoding(new BasicHeader("Content-Type", "application/json"));
        return se;
    }

}
