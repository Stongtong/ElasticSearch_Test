package com.es.test01;

import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// 第一种获取client的方式
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", "elastic1")
				// .put("cluster.name", "elastic_server")
				// .put("client.transport.ignore_cluster_name", true)
				// .put("client.transport.ping_timeout", 5)
				// .put("client.transport.nodes_sampler_interval", 5)
				.build();
		@SuppressWarnings("resource")
		Client client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(
						"192.168.1.26", 9300));
		// .addTransportAddress(new InetSocketTransportAddress("127.0.0.1",
		// 9300));

		// 第二种获取client的方式
		/*
		 * Node node = NodeBuilder.nodeBuilder() .clusterName("es_cluster")
		 * .client(true) .data(false) .local(true) .node(); Client client =
		 * node.start().client();
		 */

		// 统计总共拨打次数
		SearchResponse totalResponse = client.prepareSearch("phone_call")
				.setTypes("unicom")
				// .setQuery(QueryBuilders.matchQuery("被叫号码", "10010"))
				.execute().actionGet();
		// 命中总数totalHits
		long totalHits = totalResponse.getHits().getTotalHits(); // 这是索引中记录总数，而totalResponse.getHits().totalHits()是命中纪录数
		System.out.println("拨打总次数：" + totalHits);

		// 统计不同号码个数
		@SuppressWarnings("rawtypes")
		MetricsAggregationBuilder aggregation1 = AggregationBuilders
				.cardinality("agg1").field("被叫号码");
		SearchResponse numResponse1 = client.prepareSearch("phone_call")
				.setTypes("unicom").addAggregation(aggregation1).execute()
				.actionGet();
		Cardinality agg1 = numResponse1.getAggregations().get("agg1");
		// 主叫用户总数
		long user_size1 = agg1.getValue();
		System.out.println("被叫用户总数：" + user_size1);

		// 统计各号码分别拨打次数
		TermsBuilder termsBuilders1 = AggregationBuilders.terms("phoned_number")
				.field("被叫号码");

		SearchResponse timesResponse1 = client.prepareSearch("phone_call")
				.setTypes("unicom").addAggregation(termsBuilders1).setSize(Integer.MAX_VALUE)
				.execute().actionGet();
		Terms timesTerms1 = timesResponse1.getAggregations().get("phoned_number");
		System.out.println("分页大小：" + timesTerms1.getBuckets().size() + "	结果数量："
				+ timesResponse1.getHits().totalHits());
		System.out.println("各号码分别拨打次数:");

		long _total1 = 0; // 统计电话总数，例如前10个电话号码总共拨打次数
		int _size1 = timesTerms1.getBuckets().size(); // 电话号码个数，前10个就是10
		

		
		
		MetricsAggregationBuilder aggregation = AggregationBuilders
				.cardinality("agg").field("主叫号码");
		SearchResponse numResponse = client.prepareSearch("phone_call")
				.setTypes("unicom").addAggregation(aggregation).execute()
				.actionGet();
		Cardinality agg = numResponse.getAggregations().get("agg");
		// 主叫用户总数
		long user_size = agg.getValue();
		System.out.println("主叫用户总数：" + user_size);

		// 统计各号码分别拨打次数
		TermsBuilder termsBuilders = AggregationBuilders.terms("phoneing_number")
				.field("主叫号码");

		SearchResponse timesResponse = client.prepareSearch("phone_call")
				.setTypes("unicom").addAggregation(termsBuilders).setSize(Integer.MAX_VALUE)
				.execute().actionGet();
		Terms timesTerms = timesResponse.getAggregations().get("phoneing_number");
		System.out.println("分页大小：" + timesTerms.getBuckets().size() + "	结果数量："
				+ timesResponse.getHits().totalHits());
		System.out.println("各号码分别拨打次数:");

		long _total = 0; // 统计电话总数，例如前10个电话号码总共拨打次数
		int _size = timesTerms.getBuckets().size(); // 电话号码个数，前10个就是10


		for (Terms.Bucket entry : timesTerms.getBuckets()) {
			for (Terms.Bucket entry1 : timesTerms1.getBuckets()) {
				
				if((entry.getKey()).equals((entry1).getKey())){
					
					System.out.println("用户号码：" + entry.getKey() + "	主叫次数："
						+ entry.getDocCount()+"    被叫次数"+entry1.getDocCount());
				}
				
			}
//			
//			System.out.println("用户号码：" + entry.getKey() + "	主叫次数："
//					+ entry.getDocCount());
//			_total += entry.getDocCount();
		}

		// 计算平均次数
		long avg_count = (_total / _size);
		
		
		System.out.println("前x个用户平均拨打次数：" + avg_count);

		// 简单的matchQuery搜索方式
		// SearchResponse keyResponse = client.prepareSearch("phone_call") //
		// 索引名称
		// .setTypes("unicom") // 索引类型
		// .setQuery(QueryBuilders.matchQuery("被叫号码", "10010")) // 设置某种搜索
		// .setSize(5).execute().actionGet();
		// SearchHit[] hits = keyResponse.getHits().getHits();
		// System.out.println("分页大小：" + keyResponse.getHits().getHits().length
		// + "	命中的记录数目：" + keyResponse.getHits().totalHits());
		// if (hits.length > 0) {
		// for (SearchHit hit : hits) {
		// String result = (String) hit.getSourceAsString();
		// System.out.println(result);
		// }
		// }
	}
}
