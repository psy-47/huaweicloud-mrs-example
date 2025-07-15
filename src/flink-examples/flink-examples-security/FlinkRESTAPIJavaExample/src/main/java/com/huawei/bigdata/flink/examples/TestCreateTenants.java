package com.huawei.bigdata.flink.examples;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.huawei.bigdata.flink.util.HttpClientUtil;
import com.huawei.bigdata.flink.util.LoginClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestCreateTenants {
    public static void main(String[] args) {
        ParameterTool paraTool = ParameterTool.fromArgs(args);
        final String hostName = paraTool.get("hostName");    // 修改hosts文件，使用主机名
        final String keytab = paraTool.get("keytab");        // user.keytab路径
        final String krb5 = paraTool.get("krb5");            // krb5.conf路径
        final String principal = paraTool.get("principal");  // 认证用户
        final String jobId = paraTool.get("jobId");          // jobId
        final String tenantId = paraTool.get("tenantId");    // tenantId
        final String jobName = paraTool.get("jobName");      // jobName
        final String udfJarId = paraTool.get("udfJarId");    // udfJarId
        final String type = paraTool.get("type");            // type
        String realUser = "flinkserveradmin";
        if (StringUtils.isNotBlank(paraTool.get("realUser"))) {
            realUser = paraTool.get("realUser");             // realUser
        }
        System.setProperty("java.security.krb5.conf", krb5);
        String httpHost = "https://" + hostName + ":28943";
        String url = httpHost + "/flink/v1/tenants";

        String url3 = httpHost + "/flink/v1/proxyUserLogin";
        String jsonStr2 = "{\n" +
                " \"realUser\":\"" + realUser + "\"\n" +
                "}";
        try {
            System.out.println("========== start login");
            LoginClient.getInstance().setConfigure(url3, principal, keytab, "");
            LoginClient.getInstance().login();
            System.out.println();
            System.out.println("========== token");
            System.out.println(LoginClient.getInstance().getToken());

            System.out.println();
            System.out.println("========== tenants list");
            System.out.println(HttpClientUtil.doGet(url, "utf-8"));

            System.out.println();
            System.out.println("========== getCurrentUser");
            String url1 = httpHost + "/flink/v1/users/getCurrentUser";
            System.out.println(HttpClientUtil.doGet(url1, "utf-8"));

            System.out.println();
            System.out.println("========== getUserTenantPermission");
            String url2 = httpHost + "/flink/v1/users/getUserTenantPermission/019019";
            System.out.println(HttpClientUtil.doGet(url2, "utf-8"));

//            System.out.println();
//            System.out.println("========== proxyUserLogin");
//            System.out.println(HttpClientUtil.doPost(url3, jsonStr2, "utf-8", true));

            System.out.println();
            System.out.println("========== jobInfo");
            String url4 = httpHost + "/flink/v1/1/jobs?jobId=" + jobId;
            System.out.println(HttpClientUtil.doGet(url4, "utf-8"));

            System.out.println();
            System.out.println("========== CheckPoint");
            String url5 = httpHost + "/flink/v1/" + tenantId + "/jobs/" + jobId + "/snapshotlist?start=1&limit=10&asc=false&order=createTime";
            final String rest = HttpClientUtil.doGet(url5 + "&type=0", "utf-8");
            System.out.println(rest);
            final Result result = JSON.parseObject(rest, Result.class);
            final List<SnapshotPath> snapshotPaths = JSON.parseArray(JSON.toJSONString(result.getResults()), SnapshotPath.class);
            final String snapshotPath = snapshotPaths.get(0).getSnapshotPath();
            System.out.println("snapshotPath: " + snapshotPath);

            System.out.println();
            System.out.println("========== SavePoint");
            System.out.println(HttpClientUtil.doGet(url5 + "&type=1", "utf-8"));

            System.out.println();
            System.out.println("========== 创建作业");
            String url6 = httpHost + "/flink/v1/" + tenantId + "/jobs";
            String jsonStr3 = "{\n" +
                    "\"jobId\": \"4\",\n" +
                    "\"jobName\": \"" + jobName + "\",\n" +
                    "\"tenantId\": \"" + tenantId + "\",\n" +
                    "\"jobType\": \"Flink SQL\",\n" +
                    "\"remark\": null,\n" +
                    "\"jobKind\": \"batch\"\n" +
                    "}";
            System.out.println(HttpClientUtil.doPost(url6, jsonStr3, "utf-8", true));

            System.out.println();
            System.out.println("========== 编辑作业");
            String url61 = httpHost + "/flink/v1/" + tenantId + "/jobs/4";
            String jsonStr61 = "{\n" +
                    "\"jobId\": \"4\",\n" +
                    "\"jobName\": \"dev_test_insert\",\n" +
                    "\"tenantId\": \"" + tenantId + "\",\n" +
                    "\"jobType\": \"Flink SQL\",\n" +
                    "\"remark\": \"Flink SQL test\",\n" +
                    "\"createUser\": \"019019\",\n" +
                    "\"jobKind\": \"batch\"\n" +
                    "}";
            System.out.println(HttpClientUtil.doPut(url61, jsonStr61, "utf-8"));

            System.out.println();
            System.out.println("========== 强制锁定作业");
            String lockUrl = httpHost + "/flink/v1/" + tenantId + "/jobs/lock";
            String lockJson = "{\n" +
                    " \"jobId\": \"" + jobId + "\",\n" +
                    " \"force\": true\n" +
                    "}";
            System.out.println(HttpClientUtil.doPost(lockUrl, lockJson, "utf-8", true));

            System.out.println();
            System.out.println("========== 开发 Flink SQL 作业");
            String url7 = httpHost + "/flink/v1/" + tenantId + "/jobs/4";
            String jsonStr5 = "{\n" +
                    "\t\"jobId\": \"4\",\n" +
                    "\t\"jobName\": \"dev_test\",\n" +
                    "\t\"yarnQueue\": \"default\",\n" +
                    "\t\"jobType\": \"Flink SQL\",\n" +
                    "\t\"jobKind\": \"batch\",\n" +
                    "\t\"remark\": \"4\",\n" +
                    "\t\"sqlText\": \"select 1 as data;\",\n" +
                    "\t\"parallelismParam\": {\n" +
                    "\t\t\"envParallelism\": \"1\",\n" +
                    "\t\t\"maxEnvParallelism\": \"2\",\n" +
                    "\t\t\"ytm\": \"1024\",\n" +
                    "\t\t\"ys\": \"1\",\n" +
                    "\t\t\"yqu\": \"default\",\n" +
                    "\t\t\"yjm\": \"1024\"\n" +
                    "\t},\n" +
                    "\t\"checkpointParam\": {\n" +
                    "\t\t\"sqlCheckpointCleanupMode\": \"false\",\n" +
                    "\t\t\"sqlMinPauseBetweenCheckpoints\": \"300000\",\n" +
                    "\t\t\"sqlCheckpointTimeout\": \"1200000\",\n" +
                    "\t\t\"sqlCheckpointEnable\": false,\n" +
                    "\t\t\"sqlCheckpointMode\": \"EXACTLY_ONCE\",\n" +
                    "\t\t\"sqlCheckpointInterval\": \"300000\",\n" +
                    "\t\t\"sqlMaxConcurrentCheckpoints\": \"\",\n" +
                    "\t\t\"usingIncrementCheckpoint\": \"true\"\n" +
                    "\t}\n" +
                    "}";
            System.out.println(HttpClientUtil.doPost(url7, jsonStr5, "utf-8", true));

            // 动作接口
            String actionUrl = httpHost + "/flink/v1/1/jobs/action";
            if (StringUtils.equals(type, "start")) {
                System.out.println();
                System.out.println("========== 启动作业");
                String startJson = "{\n" +
                        " \"jobId\": \"" + jobId + "\",\n" +
                        " \"action\":\"start\"\n" +
                        "}";
                System.out.println(HttpClientUtil.doPost(actionUrl, startJson, "utf-8", true));
            } else if (StringUtils.equals(type, "stop")) {
                System.out.println();
                System.out.println("========== 停止作业");
                String startJson = "{\n" +
                        " \"jobId\": \"" + jobId + "\",\n" +
                        " \"action\":\"stop\"\n" +
                        "}";
                System.out.println(HttpClientUtil.doPost(actionUrl, startJson, "utf-8", true));
            } else if (StringUtils.equals(type, "savePointStop")) {
                System.out.println();
                System.out.println("========== savePoint方式停止作业");
                String startJson = "{\n" +
                        " \"jobId\": \"" + jobId + "\",\n" +
                        " \"action\":\"savePointStop\"\n" +
                        "}";
                System.out.println(HttpClientUtil.doPost(actionUrl, startJson, "utf-8", true));
            } else if (StringUtils.equals(type, "resumeFromCheckpoint")) {
                System.out.println();
                System.out.println("========== 恢复重启故障作业");
                String startJson = "{\n" +
                        " \"jobId\": \"" + jobId + "\",\n" +
                        " \"action\":\"" + snapshotPath + "\"\n" +
                        "}";
                System.out.println(HttpClientUtil.doPost(actionUrl, startJson, "utf-8", true));
            } else if (StringUtils.equals(type, "deleteCheckpoint")) {
                System.out.println();
                System.out.println("========== 删除checkpoint");
                String checkpointUrl = httpHost + "/flink/v1/" + tenantId + "/jobs/" + jobId + "/snapshot";
                final Map<String, Object> body = new HashMap<>();
                final ArrayList<String> snapshotList = new ArrayList<>();
                snapshotList.add(snapshotPath);
//                body.put("jobKind", "stream"); // 非必须
                body.put("snapshotList", snapshotList);
                final String jsonString = JSON.toJSONString(body);
                System.out.println("deleteCheckpoint param: " + jsonString);
                System.out.println(HttpClientUtil.doDelete(checkpointUrl, jsonString, "utf-8"));

                System.out.println();
                System.out.println("========== 删除作业");
                String del = httpHost + "/flink/v1/" + tenantId + "/jobs/4";
                System.out.println(HttpClientUtil.doDelete(del, null, "utf-8"));
            } else if (StringUtils.equals(type, "udf")) {
                udfTest(httpHost, tenantId);
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private static void udfTest(String httpHost, String tenantId) throws Exception {
        System.out.println();
        System.out.println("========== 上传UDF Jar");
        String udfUploadUrl = httpHost + "/flink/file/udf/uploadUdfJar";
        final File file = new File("/home/app/test/udf1.jar");
        final String uploadResult = HttpClientUtil.doUpload(udfUploadUrl, tenantId, file);
        System.out.println(uploadResult);
        final JSONObject udfPath = JSON.parseObject(uploadResult);
        final String udfJarTmpLocalPath = (String) udfPath.get("udfJarTmpLocalPath");

        System.out.println();
        System.out.println("========== 创建 UDF");
        String udf1 = httpHost + "/flink/v1/" + tenantId + "/udfs";
        String udfJson = "{\n" +
                "\t\"tenantId\": \"" + tenantId + "\",\n" +
                "\t\"udfJarTmpLocalPath\": \"" + udfJarTmpLocalPath + "\",\n" +
                "\t\"description\": \"\",\n" +
                "\t\"udfList\": [\n" +
                "\t\t{\n" +
                "\t\t\t\"tenantId\": \"" + tenantId + "\",\n" +
                "\t\t\t\"udfName\": \"string_length_udf1\",\n" +
                "\t\t\t\"udfClassName\": \"com.personal.misi.StringLengthUDF\"\n" +
                "\t\t}\n" +
                "\t]\n" +
                "}";
        final String createResult = HttpClientUtil.doPost(udf1, udfJson, "utf-8", true);
        System.out.println(createResult);
        final JSONObject createJsonResult = JSON.parseObject(createResult);
        String udfJarId = (String) ((JSONObject) createJsonResult.get("result")).get("udfJarId");

        System.out.println();
        System.out.println("========== 编辑 UDF");
        String udf2 = httpHost + "/flink/v1/" + tenantId + "/udfs/" + udfJarId;
        String udfJson2 = "{\n" +
                "\t\"tenantId\": \"" + tenantId + "\",\n" +
                "\t\"udfJarId\": \"" + udfJarId + "\",\n" +
                "\t\"description\": \"\",\n" +
                "\t\"udfList\": [\n" +
                "\t\t{\n" +
                "\t\t\t\"tenantId\": \"" + tenantId + "\",\n" +
                "\t\t\t\"udfName\": \"string_length_udf2\",\n" +
                "\t\t\t\"udfClassName\": \"com.personal.misi.StringLengthUDF\"\n" +
                "\t\t}\n" +
                "\t]\n" +
                "}";
        final String updateResult = HttpClientUtil.doPut(udf2, udfJson2, "utf-8");
        System.out.println(updateResult);

        System.out.println();
        System.out.println("========== 分页查询 UDFJar 列表");
        String udf4 = httpHost + "/flink/v1/" + tenantId + "/udfs?order=updateTime&asc=false&start=0&limit=10";
        System.out.println(HttpClientUtil.doGet(udf4, "utf-8"));

        System.out.println();
        System.out.println("========== 查询 UDF 列表");
        String udf5 = httpHost + "/flink/v1/" + tenantId + "/udfs/" + udfJarId + "?tenantId=" + tenantId + "&udfJarId=" + udfJarId;
        System.out.println(HttpClientUtil.doGet(udf5, "utf-8"));

        System.out.println();
        System.out.println("========== 删除 UDF");
        String udf3 = httpHost + "/flink/v1/" + tenantId + "/udfs/" + udfJarId;
        System.out.println(HttpClientUtil.doDelete(udf3, null, "utf-8"));
    }

}
