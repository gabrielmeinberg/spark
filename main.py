#!/bin/bash
from os import listdir
import csv

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import split, regexp_extract, col
from pyspark.sql.types import StructType, StructField, StringType


def main():
    sc = SparkContext()
    sqlContext= SQLContext(sc)
    # Guardar todos os log da pasta files
    files = listdir('./files')

    schema_blank = StructType([StructField("value", StringType(), True)])

    # DataFrame vazio para unir todos os arquivos em Files
    main_df = sqlContext.createDataFrame([], schema_blank)

    for file in files: 
        path_file = './files/'+file 
        temp_df = sqlContext.read.text(path_file) 
        main_df = main_df.union(temp_df)

    main_df_format = main_df.select(regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'),
                                      regexp_extract('value', r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('timestamp'),
                                      regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias('URL'),
                                      regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('codeHTTP'),
                                      regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias('byte'))
    
    # 1. Numero de hosts unicos:
    host_uniques = main_df_format.groupBy('host').count().filter('count = 1').count()
    with open('./resultados/host_uniques.csv', 'w') as file:
        writer = csv.writer(file)
        writer.writerow([',host_uniques'])
        writer.writerow(['0,{}'.format(host_uniques)])
            
    # 2. O total de erros 404:
    main_df_format.groupBy('codeHTTP').count().filter('codeHTTP = "404"').toPandas().to_csv('./resultados/total_404.csv')
    
    # Os 5 URLs que mais causaram erro 404
    main_df_format.filter('codeHTTP = "404"').groupBy('URL').count().sort(col("count").desc()).limit(5).toPandas().to_csv('./resultados/top_five_404.csv')
    
    # 4. Quantidade de erros 404 por dia
    main_df_format.filter('codeHTTP = "404"').groupBy(main_df_format.timestamp.substr(1,11).alias('day')).count().toPandas().to_csv('./resultados/per_day_404.csv')

    # 5. O total de bytes retornados:
    main_df_format.select('byte').groupBy().sum().toPandas().to_csv('./resultados/total_bytes.csv')
    
    print('Arquivos Exportados para a pasta resultados')



if __name__ == '__main__':
    main()