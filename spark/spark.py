from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("people")\
        .getOrCreate()

    print("read dataset.csv ... ")
    path_people = "dataset.csv"
    df_people = spark.read.csv(path_people, header=True, inferSchema=True)
    df_people = df_people.withColumnRenamed("date of birth", "birth")
    df_people.createOrReplaceTempView("people")
    query = 'DESCRIBE people'
    spark.sql(query).show(20)
    
    # Query para mostrar todos los registros del dataset
    all_data = spark.sql("SELECT * FROM people")
    all_data.show(truncate=False)
    
    # Convertir todos los registros a formato JSON y guardarlos en data.json
    results = all_data.toJSON().collect()
    with open('results/data.json', 'w') as f:
         json.dump(results, f)
    
    # Generar summary.json con la sumatoria total de registros
    total_records = df_people.count()
    summary = {"total_records": total_records}
    with open("results/summary.json", "w") as summary_file:
         json.dump(summary, summary_file, indent=4)
    
    print("Archivo data.json y summary.json creados exitosamente.")
    spark.stop()
