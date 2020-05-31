package immutabledb

import java.util.concurrent.ExecutorService
import java.util.concurrent.ThreadPoolExecutor

import immutabledb._
import immutabledb.codec._
import immutabledb.operator._
import immutabledb.storage._

import scala.collection.mutable.Queue
import scala.concurrent.{Await, ExecutionContext, Future}
import java.util.concurrent.BlockingQueue

import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.HashMap
import scala.util.{Failure, Success, Try}
import java.util.concurrent.ExecutorService

import immutabledb._
import immutabledb.engine._
import immutabledb.storage._
import immutabledb.operator._
import scala.concurrent.duration.Duration

//object Test_10m extends App with LazyLogging {
//    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
//
//    val th = java.util.concurrent.Executors.newFixedThreadPool(4)
//    val sm = new SegmentManager(DevEnv.config.dataDir)
//    val table = sm.getTable("test_10m")
//    val engine = new Engine(sm, th)
//
//    val query = Query(
//        table.name,
//        Select("age", GT(18)),
//        Project(List("id", "age", "state"), 10)
//    )
//    val res = engine.execute(query)
//    val result = Await.result(res, Duration.Inf)
//    result.right.get.foreach(x => println(x))
//    logger.info("done")
//
////    val res1 = engine.execute(query)
////    val result1 = Await.result(res1, Duration.Inf)
////    result1.foreach(println)
////    logger.info("done")
////
////    val res2 = engine.execute(query)
////    val result2 = Await.result(res2, Duration.Inf)
////    result2.foreach(println)
////    logger.info("done")
//}

object Test_1m_project extends App with LazyLogging {
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

    val th = java.util.concurrent.Executors.newFixedThreadPool(4)
    val sm = new SegmentManager(DevEnv.config.dataDir)
    val table = sm.getTable("test_100")
    val engine = new Engine(sm, th)

    val query = Query(
        table.name,
            NoSelect,
        Project(List("id", "age", "state"))
//        ProjectAgg(List(Count("id")))
    )
    val res = engine.execute(query)
    res.fold(
        err => err.printStackTrace(),
        res => while (res.hasNext) println(res.next)
    )
    logger.info("done")
}

//object Test_100m extends App with LazyLogging {
//    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
//
//    val th = java.util.concurrent.Executors.newFixedThreadPool(4)
//    val sm = new SegmentManager(DevEnv.config.dataDir)
//    val table = sm.getTable("test_100m")
//    val engine = new Engine(sm, th)
//
//    val query = Query(
//        table.name,
//        Select("age", GT(18)),
//        ProjectAgg(List(Min("age"), Max("age")), List("state"))
////        Project(List("id", "age", "state"), 100)
//    )
//    val result = Await.result(engine.execute(query), Duration.Inf)
//    result.left.get.printStackTrace()
////    result.right.get.foreach(x => println(x))
//    logger.info("done")
//
////    val result1 = Await.result(engine.execute(query), Duration.Inf)
////    logger.info("done")
////
////    val result2 = Await.result(engine.execute(query), Duration.Inf)
////    logger.info("done")
//
////    val res1 = engine.execute(query)
////    val result1 = Await.ready(res1, Duration.Inf)
////    result1.foreach(println)
////    logger.info("done")
////
////    val res2 = engine.execute(query)
////    val result2 = Await.ready(res1, Duration.Inf)
////    result2.foreach(println)
////    logger.info("done")
//}
