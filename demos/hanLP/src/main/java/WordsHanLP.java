import com.hankcs.hanlp.HanLP;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
public class WordsHanLP {

    public static void main(String[] args) {
        //请求头中的token
        String token="010ae85ac27c4e3087ee53134e73b2941604544319987token";
        //申请的接口地址
        String url="http://comdo.hanlp.com/hanlp/v1/textAnalysis/classification";
        //所有参数
        Map<String,Object> params=new HashMap<String,Object>();
        params.put("text", "中新网8月19日电 19日，国务院联防联控机制就秋冬季疫情防控、医疗卫生工作者在抗击新冠肺炎疫情中发挥的作用情况举行发布会。国家卫健委医政医管局监察专员郭燕红表示，各地要主动关心关爱医务人员，采取更为有力的措施提高和保障他们的薪酬待遇，拓展发展空间，加强教育培训，营造更好的职业环境，建立医务人员荣誉制度，为他们安心工作和成长进步创造更为有利的条件。");
        //执行api
        String result=doHanlpApi(token,url,params);
        System.out.println(result);
    }
    public static String doHanlpApi(String token,String url,Map<String,Object> params) {
        // 创建Httpclient对象
        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        String resultString = "";
        try {
            // 创建Http Post请求
            HttpPost httpPost = new HttpPost(url);
            //添加header请求头，token请放在header里
            httpPost.setHeader("token", token);
            // 创建参数列表
            List<NameValuePair> paramList = new ArrayList<NameValuePair>();
            if (params != null) {
                for (String key : params.keySet()) {
                    //所有参数依次放在paramList中
                    paramList.add(new BasicNameValuePair(key, (String) params.get(key)));
                }
                //模拟表单
                UrlEncodedFormEntity entity = new UrlEncodedFormEntity(paramList, "utf-8");
                httpPost.setEntity(entity);
            }
            // 执行http请求
            response = httpClient.execute(httpPost);
            resultString = EntityUtils.toString(response.getEntity(), "utf-8");
            return resultString;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(response!=null) {
                try {
                    response.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

}
