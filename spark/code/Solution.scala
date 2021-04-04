import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Row, SaveMode, SparkSession, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


import sqlContext.implicits._
// this function will help you to explode the nested array and struct on a single function call on a dataframe
def flatendata(df: DataFrame): DataFrame = {
    //getting all the fields from schema
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    //length shows the number of fields inside dataframe
    val length = fields.length
    // looping throught the length of the fields of the data frame
    for (i <- 0 to fields.length - 1) {
      val field = fields(i)
      val fieldtype = field.dataType
      val fieldName = field.name
      fieldtype match {
        // this will execute only when the nested type is array
        case arrayType: ArrayType =>
          val fieldName1 = fieldName
          val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName1)
          // exploding the the array and get the all the filed along with the nulls (since we used explode_outer)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName1) as $fieldName1")
          //val fieldNamesToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName1.*"))
          val explodedDf = df.selectExpr(fieldNamesAndExplode: _*)
          return flatendata(explodedDf)

        case structType: StructType =>
          // since children in struct, dynamical giving the childnames
          val childFieldnames = structType.fieldNames.map(childname => fieldName + "." + childname)
          val newfieldNames = fieldNames.filter(_ != fieldName) ++ childFieldnames
          // renaming the column and replacing the column sting
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_").replace("$", "_").replace("__", "_").replace(" ", "").replace("-", ""))))
          val explodedf = df.select(renamedcols: _*)
          return flatendata(explodedf)
        case _ =>
      }
    }
    df
  }


//reading the data from a location , load as json data
val df = spark.read.json("/FileStore/tables/part_r_00000_ace4d84a_d8cb_4eaa_a377_9d3e8bbf73b2-1")
//data is getting flatten using the above udf;
val flatten_df = flatendata(df)

//cahed this data since it is small set and used more than once in the transformation
flatten_df.cache()

display(flatten_df)

// udf: concating three columns, created this function since i used more than once
val getConcatenated = udf( (parent: String, child: String,child2: String) => { parent + "," + child + "," + child2 } )
val concat_data =  flatten_df.
                    withColumn("allindustries",getConcatenated($"industry",$"children_industry",$"children_children_industry")).
                    withColumn("allratings",getConcatenated($"rating",$"children_rating",$"children_children_rating")).
                    select($"allindustries",$"allratings")

// udf: This function is to a array object to exploded the data
val replaceInArr = udf((jsonArrStr: String) => {
  jsonArrStr.replace("[", "").replace("\"", "").replace("]", "").split(",")
})

// udf: This function would help in exploding the two column data with sequence
val explode_data = udf((xs:Seq[String],ys:Seq[String]) => xs.zip(ys))
//exploding single row of data into multiple, using the above udf
val explode_concat_data = concat_data.withColumn("list",explode(explode_data(replaceInArr($"allindustries").cast("array<string>"),replaceInArr($"allratings").cast("array<string>")))).
                          select($"list._1".alias("Industries"),$"list._2".cast("Int").alias("Ratings"))

//finding the max rating industries
val finding_industries_with_max_rating = explode_concat_data.filter($"Industries" =!= "null").groupBy($"Industries").max("Ratings")


display(finding_industries_with_max_rating)


val union_parent_child = flatten_df.select ($"name".alias("ParentCompany"),$"children_name".alias("ChildCompany"),$"children_rating".alias("ChildRating")).filter($"ChildCompany" =!= "null").union(flatten_df.select ($"children_name".alias("ParentCompany"),$"children_children_name".alias("ChildCompany"),$"children_children_rating".cast("Int").alias("ChildRating")).filter($"ChildCompany" =!= "null")).distinct()

// creating the window function
val childRank = Window.partitionBy('ParentCompany).orderBy('ChildRating.asc)

// getting the minmum child rating and assocaitaed parent value
val finding_min_child_company = union_parent_child.withColumn("rank", rank.over(childRank)).filter($"rank" === 1).drop($"rank")

display(finding_min_child_company)
