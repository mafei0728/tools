package com.mafei.rdd

import java.sql.{Connection, DriverManager}

import com.mafei.utils.SparkContextFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.commons.lang3.StringUtils

/*
 * @Description
 * @Author  mafei0728
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2020/9/17
 */

case class Job_Detail(job_id: String, job_name: String, job_url: String, job_location: String, job_salary: String,
                      job_company: String, job_experience: String, job_class: String, job_given: String,
                      job_detail: String, company_type: String, company_person: String,
                      search_key: String, city: String) extends Ordered[Job_Detail] {
  override def compare(that: Job_Detail): Int = {
    def getSalary(x: Job_Detail): Int = {
      if (StringUtils.isNoneEmpty(x.job_salary)
        && StringUtils.containsAny(x.job_salary, 'k', 'K') // 包含大小写
        && x.job_salary.contains("-")
        && x.job_salary.split("-").length == 2) {
        x.job_salary.replaceAll("[kK]", "") // 替换大小写
          .split("-")(1).toInt
      } else {
        0
      }
    }

    getSalary(this) - getSalary(that)
  }
}

object RddExam02 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc: SparkContext = SparkContextFactory.sc
  val getConn: () => Connection = () => {
    DriverManager.getConnection("jdbc:mysql://hadoop01:3306/ma_test?characterEncoding=UTF-8",
      "root",
      "mafei0728")
  }
  val sql_text: String = "select * from jobdetail where job_id >= ? AND job_id <= ?"
  val jdbcRdd: JdbcRDD[Job_Detail] = new JdbcRDD[Job_Detail](sc,
    getConn, sql_text,
    1,
    75000,
    8,
    rs => {
      val job_id = rs.getString(1)
      val job_name: String = rs.getString(2)
      val job_url = rs.getString(3)
      val job_location: String = rs.getString(4)
      val job_salary = rs.getString(5)
      val job_company: String = rs.getString(6)
      val job_experience = rs.getString(7)
      val job_class: String = rs.getString(8)
      val job_given = rs.getString(9)
      val job_detail: String = rs.getString(10)
      val company_type = rs.getString(11)
      val company_person: String = rs.getString(12)
      val search_key = rs.getString(13)
      val city: String = rs.getString(14)
      Job_Detail(job_id, job_name, job_url, job_location, job_salary, job_company, job_experience, job_class, job_given, job_detail, company_type, company_person, search_key, city)
    }
  )
  // 按照岗位分组
  val rdd01: RDD[(String, Iterable[Job_Detail])] = jdbcRdd.groupBy(x => x.search_key)

  // 求每个职位的人数
  /*
   * @Description: 求每个职业人数
   * @param :
   * @return void
   * @author: mafei0728
   * @date: 2020/9/17 21:45
   */
  def getCountForJob(): Unit = {
    val rdd02: RDD[(String, Int)] = jdbcRdd.map(x => (x.search_key, 1)).reduceByKey(_ + _).filter(_._1 != null).repartition(2)
    // 写入数据库
    rdd02.foreachPartition(iter => {
      val conn: Connection = DriverManager.getConnection("jdbc:mysql://hadoop01:3306/ma_test?characterEncoding=UTF-8",
        "root",
        "mafei0728")
      conn.setAutoCommit(false)
      val prm = conn.prepareStatement("insert into job_count (search_name, job_num) values (?, ?)")
      iter.foreach(record => {
        prm.setString(1, record._1)
        prm.setInt(2, record._2)
        prm.addBatch()
      }
      )
      prm.executeBatch()
      conn.commit()
      conn.setAutoCommit(true)
      prm.close()
      conn.close()
    })
  }

  def getMaxMinSalary(): Unit = {
    val res: RDD[(Job_Detail, Job_Detail)] = rdd01.map(x => {
      val rez = x._2.toList.sorted
      (rez.head, rez.last)
    })
    res.collect().foreach(x => println("min " + x._1.job_id + "\t" + x._1.job_salary + "\t" + x._1.search_key
      + "\n" + "max " + x._2.job_id + "\t" + x._2.job_salary + "\t" + x._2.search_key + "\n-----------\n"))

  }


  def main(args: Array[String]): Unit = {
    getMaxMinSalary()
  }

}
