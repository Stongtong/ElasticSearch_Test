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

		// ��һ�ֻ�ȡclient�ķ�ʽ
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

		// �ڶ��ֻ�ȡclient�ķ�ʽ
		/*
		 * Node node = NodeBuilder.nodeBuilder() .clusterName("es_cluster")
		 * .client(true) .data(false) .local(true) .node(); Client client =
		 * node.start().client();
		 */

		// ͳ���ܹ��������
		SearchResponse totalResponse = client.prepareSearch("phone_call")
				.setTypes("unicom")
				// .setQuery(QueryBuilders.matchQuery("���к���", "10010"))
				.execute().actionGet();
		// ��������totalHits
		long totalHits = totalResponse.getHits().getTotalHits(); // ���������м�¼��������totalResponse.getHits().totalHits()�����м�¼��
		System.out.println("�����ܴ�����" + totalHits);

		// ͳ�Ʋ�ͬ�������
		@SuppressWarnings("rawtypes")
		MetricsAggregationBuilder aggregation1 = AggregationBuilders
				.cardinality("agg1").field("���к���");
		SearchResponse numResponse1 = client.prepareSearch("phone_call")
				.setTypes("unicom").addAggregation(aggregation1).execute()
				.actionGet();
		Cardinality agg1 = numResponse1.getAggregations().get("agg1");
		// �����û�����
		long user_size1 = agg1.getValue();
		System.out.println("�����û�������" + user_size1);

		// ͳ�Ƹ�����ֱ𲦴����
		TermsBuilder termsBuilders1 = AggregationBuilders.terms("phoned_number")
				.field("���к���");

		SearchResponse timesResponse1 = client.prepareSearch("phone_call")
				.setTypes("unicom").addAggregation(termsBuilders1).setSize(Integer.MAX_VALUE)
				.execute().actionGet();
		Terms timesTerms1 = timesResponse1.getAggregations().get("phoned_number");
		System.out.println("��ҳ��С��" + timesTerms1.getBuckets().size() + "	���������"
				+ timesResponse1.getHits().totalHits());
		System.out.println("������ֱ𲦴����:");

		long _total1 = 0; // ͳ�Ƶ绰����������ǰ10���绰�����ܹ��������
		int _size1 = timesTerms1.getBuckets().size(); // �绰���������ǰ10������10
		

		
		
		MetricsAggregationBuilder aggregation = AggregationBuilders
				.cardinality("agg").field("���к���");
		SearchResponse numResponse = client.prepareSearch("phone_call")
				.setTypes("unicom").addAggregation(aggregation).execute()
				.actionGet();
		Cardinality agg = numResponse.getAggregations().get("agg");
		// �����û�����
		long user_size = agg.getValue();
		System.out.println("�����û�������" + user_size);

		// ͳ�Ƹ�����ֱ𲦴����
		TermsBuilder termsBuilders = AggregationBuilders.terms("phoneing_number")
				.field("���к���");

		SearchResponse timesResponse = client.prepareSearch("phone_call")
				.setTypes("unicom").addAggregation(termsBuilders).setSize(Integer.MAX_VALUE)
				.execute().actionGet();
		Terms timesTerms = timesResponse.getAggregations().get("phoneing_number");
		System.out.println("��ҳ��С��" + timesTerms.getBuckets().size() + "	���������"
				+ timesResponse.getHits().totalHits());
		System.out.println("������ֱ𲦴����:");

		long _total = 0; // ͳ�Ƶ绰����������ǰ10���绰�����ܹ��������
		int _size = timesTerms.getBuckets().size(); // �绰���������ǰ10������10


		for (Terms.Bucket entry : timesTerms.getBuckets()) {
			for (Terms.Bucket entry1 : timesTerms1.getBuckets()) {
				
				if((entry.getKey()).equals((entry1).getKey())){
					
					System.out.println("�û����룺" + entry.getKey() + "	���д�����"
						+ entry.getDocCount()+"    ���д���"+entry1.getDocCount());
				}
				
			}
//			
//			System.out.println("�û����룺" + entry.getKey() + "	���д�����"
//					+ entry.getDocCount());
//			_total += entry.getDocCount();
		}

		// ����ƽ������
		long avg_count = (_total / _size);
		
		
		System.out.println("ǰx���û�ƽ�����������" + avg_count);

		// �򵥵�matchQuery������ʽ
		// SearchResponse keyResponse = client.prepareSearch("phone_call") //
		// ��������
		// .setTypes("unicom") // ��������
		// .setQuery(QueryBuilders.matchQuery("���к���", "10010")) // ����ĳ������
		// .setSize(5).execute().actionGet();
		// SearchHit[] hits = keyResponse.getHits().getHits();
		// System.out.println("��ҳ��С��" + keyResponse.getHits().getHits().length
		// + "	���еļ�¼��Ŀ��" + keyResponse.getHits().totalHits());
		// if (hits.length > 0) {
		// for (SearchHit hit : hits) {
		// String result = (String) hit.getSourceAsString();
		// System.out.println(result);
		// }
		// }
	}
}
