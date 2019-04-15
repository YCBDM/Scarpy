from elasticsearch import Elasticsearch

# 默认host为localhost,port为9200.但也可以指定host与port
# es = Elasticsearch('10.197.236.171')

# 添加或更新数据,index，doc_type名称可以自定义，id可以根据需求赋值,body为内容
# es.index(index="hikbidtada",doc_type="Article",id=1,body={"title":"First Title","author":"First Author",
#                                                           "publish_time":"2017年07月31日 20:39:16",
#                                                           "url":"https://www.elastic.co/downloads/elasticsearch"})

# 或者:ignore=409忽略文档已存在异常
# es.create(index="hikbidtada",doc_type="Article",id=1,ignore=409,body={"name":"python","addr":"深圳"})


# 获取索引为my_index,文档类型为test_type的所有数据,result为一个字典类型
#result = es.search(index="hikbidtada",doc_type="Article")

# 或者这样写:搜索id=1的文档
result = es.get(index="hikbidtada",doc_type="Article",id=1)

# 打印所有数据
# for item in result["hits"]["hits"]:
#    print(item["_source"])

# 删除id=1的数据
# result = es.delete(index="hikbidtada",doc_type="Article",id=1)