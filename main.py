# Подразумевается, что "вернуть имена всех продуктов, у которых нет категорий" в том же датафрейме
# эквивалентно выводу вида "Имя продукта - NULL"

def get_product_category_pairs(products_df, categories_df, product_category_df):
    return (
        products_df
          .join(product_category_df, "product_id", "left")  # Присоединяем id связанных категорий (или NULL, если связи нет)
          .join(categories_df, "category_id", "left")       # Присоединяем имена категорий
          .select("product_name", "category_name")          # Отбираем имена продуктов и категорий
    )



# Тестируем
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local').appName('task2_spark').getOrCreate()


# Тестовые данные
products_data = [(0, "apple"), (1, "banana"), (2, "milk"), (3, "onion")]
categories_data = [(0, "category0"), (1, "category1"), (2, "category2"), (3, "category3")]
product_category_data = [(0, 1), (0, 2), (1, 1), (1, 3)]

products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
product_category_df = spark.createDataFrame(product_category_data, ["product_id", "category_id"])



# Вызов функции
result_df = get_product_category_pairs(products_df, categories_df, product_category_df)
result_df.show()



# Результат
# +------------+-------------+
# |product_name|category_name|
# +------------+-------------+
# |       apple|    category2|
# |       apple|    category1|
# |      banana|    category3|
# |      banana|    category1|
# |       onion|         NULL|
# |        milk|         NULL|
# +------------+-------------+
