package mycrawler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.nutch.crawl.DbUpdaterJob;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.InjectorJob;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.parse.ParserJob;

import mycrawler.Myhbase;

public class Testcrawler {

    private static final String URL = "https://detail.ju.taobao.com/home.htm?id=10000022178228&item_id=531115135635";  
    private static final String ECODING = "GBK";  
    // 获取img标签正则  
//    private static final String IMGURL_REG = "<img.*src=(.*?)[^>]*?>";  
    // 获取src路径的正则  
    private static final String IMGSRC_REG = "http:\"?(.*?)(\"|>|\\s+)";
    private static String[] content = new String[10];
	private static String[] args1 = new String[1];
	private static String[] args2 = new String[2];
	private static String[] args3 = new String[1];
	
public static  void main(String[] args) throws Exception {
			args3[0]="/home/qyb/apache-nutch-2.3/urls/seed.txt";
			args2[0]="-topN";
			args2[1]="10";
			args1[0]="-all";
			InjectorJob.main(args3);
			System.out.println("depth = 1......");
			GeneratorJob.main(args2);
			FetcherJob.main(args1);
			ParserJob.main(args1);
			DbUpdaterJob.main(args1);
			System.out.println("depth = 2......");
			GeneratorJob.main(args2);
			FetcherJob.main(args1);
			ParserJob.main(args1);
			DbUpdaterJob.main(args1);
			System.out.println("depth = 3......");
			GeneratorJob.main(args2);
			FetcherJob.main(args1);
			ParserJob.main(args1);
			DbUpdaterJob.main(args1);
//	Testcrawler tc = new Testcrawler();
//		String HTML = tc.getHTML(URL);
//        List<String> Imgurls = tc.getImageUrl(HTML);
//        if(HTML.isEmpty())
//        	System.out.println("there is nothing");
//        else System.out.println(HTML);
        
//        if(Imgurls.isEmpty())
//        	System.out.println("there is nothing");
//        else 
//        int index=0;
//        	while(index < Imgurls.size()){
//        		System.out.println(Imgurls.get(index)+"\n");
//        		index++;
//        	}
//        Myhbase.get("webpage", "com.taobao.ju:https/tg/forecast.htm");
//			Myhbase.deleteRow("webpage", "com.tmall.detail:https/item.htm?id=531115135635&tracelog=jubuybigpic");
			Myhbase.QueryByCondition("commodity");
//			String text = Myhbase.QueryByCondition1("webpage");
//			String[] k =  text.split(" ");
//			
//			System.out.println(k[0]);
//			System.out.println(text);

	content = new String[]{
			"18780052931",
			"雀巢法国原装进口金牌咖啡100g*2（每个ID限购20件）",
			"食品",
			"包邮，送一对爱恋指环杯",
			"203.20",
			"142.00",
			"http://detail.tmall.com/item.htm?id=18780052931&tracelog=jubuybigpic",
			"http://detail.ju.taobao.com/home.htm?id=10000021722005&item_id=18780052931",
			"月销量1198"
	};
	
//	Myhbase.put("commodity",content[0],"n","name",content[1]);
//	Myhbase.put("commodity",content[0],"c","category",content[2]);
//	Myhbase.put("commodity",content[0],"fm","favorable_means",content[3]);
	
//	Myhbase.put("commodity",content[0],"fm","free_shipping","包邮");
//	Myhbase.put("commodity",content[0],"fm","return_freight","退货赔运费");
//	Myhbase.put("commodity",content[0],"fm","gift","赠送礼品");
////	Myhbase.put("commodity",content[0],"fm","special_offer","特价");
//	
//	Myhbase.put("commodity",content[0],"p","original_price",content[4]);
//	Myhbase.put("commodity",content[0],"p","favorable_price",content[5]);
//	Myhbase.put("commodity",content[0],"u","taobao_url",content[6]);
//	Myhbase.put("commodity",content[0],"u","juhuasuan_url",content[7]);
//	Myhbase.put("commodity",content[0],"s","sales_before",content[8]);
//	Myhbase.put("commodity",content[0],"s","sales_after",content[9]);

    }

/*** 
     * 获取HTML内容 
     *  
     * @param url 
     * @return 
     * @throws Exception 
     */  
    private String getHTML(String url) throws Exception {  
        URL uri = new URL(url);
        URLConnection connection = uri.openConnection();
        InputStream in = connection.getInputStream();
        byte[] buf = new byte[1024];
        int length = 0;
        StringBuffer sb = new StringBuffer();
        while ((length = in.read(buf, 0, buf.length)) > 0) {
            sb.append(new String(buf, ECODING));
        }
        in.close();
        return sb.toString();
    }
    
//    /*** 
//     * 获取ImageUrl地址 
//     *  
//     * @param HTML 
//     * @return 
//     */  
//    private List<String> getImageUrl(String HTML) {  
//        Matcher matcher = Pattern.compile(IMGURL_REG).matcher(HTML);  
//        List<String> listImgUrl = new ArrayList<String>();  
//        while (matcher.find()) {  
//            listImgUrl.add(matcher.group());  
//        }  
//        return listImgUrl;
//    }
}
