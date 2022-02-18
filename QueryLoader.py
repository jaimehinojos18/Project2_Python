from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, when, to_date, date_format, round, lag, coalesce, log


class query_loader:
    def __init__(self):
        self.spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
        self.covid_data = self.initialize_data().withColumnRenamed("Country/Region", "Country_Region").withColumnRenamed("Province/State", "Province_State")
        self.max_deaths = self.covid_data.select(col("Country_Region"),col("Deaths").cast("Int")).groupBy("Country_Region").sum("Deaths")
        self.pop_data = self.spark.read.option("header","true").csv("data/population_by_country_2020.csv")
        self.death_join_pop = self.max_deaths.join(self.pop_data, self.covid_data.Country_Region == self.pop_data.Country, "inner")\
            .select(col("Country"),col("sum(Deaths)"),col("Population").cast("Int"))
        self.country_by_months = self.country_by_month()
        self.monthly_data = self.get_monthly()
        self.continent = self.spark.read.option("header", "true").csv("data/continents.csv").withColumnRenamed("Country/Region", "Country_Region")
        self.covid_continents = self.covid_data.join(self.continent, "Country_Region")


    def q1(self):
        return self.monthly_data.select("Date", "`Mortality Rate`", "`Spread Rate`", "`Difference`").orderBy("Date")

    def q2(self):
        return self.monthly_data
    def q3(self):
        return self.monthly_data.select("Date", "Confirmed", "Deaths", "Recovered").orderBy("Date")
    def q4(self):
        return self.monthly_data.select("Date", "`Mortality Rate`").withColumn("Mortality Rate",round(col("`Mortality Rate`")*100, 2)).orderBy("Date")
    def q5(self):
        return self.country_by_months.withColumnRenamed("sum(Deaths)","Deaths").withColumn("Deaths",col("Deaths").cast("Int"))\
            .groupBy("Country_Region").sum("Deaths").withColumnRenamed("sum(Deaths)","Deaths")\
            .orderBy(col("Deaths").desc())
    def q6(self):
        return self.country_by_months.withColumnRenamed("sum(Deaths)","Deaths").withColumn("Deaths",col("Deaths").cast("Int"))\
            .groupBy("Country_Region").sum("Deaths")\
            .orderBy(col("sum(Deaths)").asc())\
            .filter(col("sum(Deaths)").isNotNull())
    def q7(self):
        df = self.covid_data.select(
            col("Date"),
            col("Country_Region"),
            col("Confirmed").cast("long")
            ).withColumn("Date", to_date(col("Date"), "MM/dd/yyyy"))
        df = df.groupBy(col("Date")).sum("Confirmed").orderBy( "Date").withColumnRenamed("sum(Confirmed)", "Confirmed")\
            .withColumn("Difference", coalesce(col("Confirmed")-lag("Confirmed", 1).over(Window.partitionBy().orderBy("Date")), col("Confirmed")))\
            .na.fill(0).withColumn("DayOfWeek", to_date(col("Date"), "MM/dd/yyyy"))\
            .withColumn("DayOfWeek", date_format(col("DayOfWeek"), "E"))\
            .filter(col("Date").isNotNull())
        return df
    def q8(self):
        deaths_conts = self.death_join_pop.join(self.continent, self.death_join_pop.Country == self.continent.Country_Region).drop("Country_Region")
        modified = deaths_conts.withColumn("sum(Deaths)", log("sum(Deaths)"))\
            .withColumn("Population", log("Population"))
        print("Correlation Value: " + str(modified.stat.corr("Population", "sum(Deaths)")))
        return modified.sort(col("Population").desc_nulls_last()).filter(col("sum(Deaths)").isNotNull())





    def data(self, i):
        if i == 1:
            return self.q1()
        elif i ==2:
            return self.q2()
        elif i ==3:
            return self.q3()
        elif i ==4:
            return self.q4()
        elif i ==5:
            return self.q5()
        elif i ==6:
            return self.q6()
        elif i ==7:
            return self.q7()
        elif i ==8:
            return self.q8()



    def initialize_data(self):
        return self.spark.read.option("header","true").csv("data/covid_daily_differences.csv")\
            .withColumn("Date", to_date(col("Date")))\
            .withColumn("Confirmed",col("Confirmed").cast("long"))\
            .withColumn("Confirmed",col("Confirmed").cast("int"))\
            .withColumn("Deaths",col("Deaths").cast("int"))\
            .withColumn("Recovered",col("Recovered").cast("int"))\
            .withColumn("Recovered", when(col("Recovered") < 0, 0).otherwise(col("Recovered")))\
            .withColumn("Deaths", when(col("Deaths") < 0, 0).otherwise(col("Deaths")))\
            .withColumn("Confirmed", when(col("Confirmed") < 0, 0).otherwise(col("Confirmed")))
    def country_by_month(self):
        n_df = self.covid_data.withColumn("Date", date_format(col("Date"),"yyyy-MM"))
        n_df = n_df.groupBy("Country_Region", "Date").sum("Confirmed", "Deaths", "Recovered")\
            .orderBy("Date").filter("Date IS NOT NULL")
        n_df.withColumnRenamed("sum(Deaths)", "Deaths").\
            withColumnRenamed("sum(Confirmed)", "Confirmed").withColumnRenamed("sum(Recovered)", "Recovered")
        return n_df

    def get_monthly(self):
        covid = self.spark.read.option("header", "true").csv("data/covid_19_data_cleaned.csv")
        months = covid.withColumn("Date", to_date(col("Date"), "MM/dd/yyyy"))
        months = months.withColumn("Confirmed", col("Confirmed").cast("int")).withColumn(
            "Deaths", col("Deaths").cast("int")
        ).withColumn(
            "Recovered", col("Recovered").cast("int")
        )
        months = months.groupBy("Date").sum("Confirmed", "Deaths", "Recovered").sort("Date").withColumn(
            "Mortality Rate",
            round(col("sum(Deaths)") / col("sum(Confirmed)"), 3)).withColumnRenamed("sum(Deaths)", "Deaths"). \
            withColumnRenamed("sum(Confirmed)", "Confirmed").withColumnRenamed("sum(Recovered)", "Recovered")
        months = months.select(date_format(col("Date"), "yyyy-MM").alias("Date"), col("Confirmed"), col("Deaths"),
                               col("Recovered"))
        months = months.groupBy("Date").max("Confirmed", "Deaths", "Recovered").orderBy("Date").withColumnRenamed(
            "max(Deaths)", "Deaths").\
            withColumnRenamed("max(Confirmed)", "Confirmed").withColumnRenamed("max(Recovered)", "Recovered")
        months = months.withColumn("Mortality Rate", round(col("Deaths") / col("Confirmed"), 3))
        months = months.withColumn("Spread Rate",
                                   round((col("Confirmed") - lag("Confirmed", 1).over(Window.partitionBy().orderBy("Date"))) / lag(
                                       "Confirmed", 1).over(Window.partitionBy().
                                                            orderBy("Date")), 3)).withColumn("Difference", coalesce(
            col("Confirmed") - lag("Confirmed", 1).over(Window.partitionBy().
                                                        orderBy("Date")), col("Confirmed"))).na.fill(0)
        return months
