import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import geotrellis.spark._

import geotrellis.raster._
import geotrellis.vector._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io._
import geotrellis.spark.tiling.LayoutDefinition

import geotrellis.raster.mapalgebra.focal.Square
import geotrellis.raster.io.geotiff._
import geotrellis.shapefile.ShapeFileReader

import java.io.File
import com.github.tototoshi.csv._

// import geotrellis.raster.resample.{ResampleMethod, NearestNeighbor}
// import geotrellis.raster.mapalgebra._

object Combiner {
  def main(args: Array[String]) {

    if (args.length != 4) {
      System.err.println(s"""
        |Usage: Indexer <mobile>
        |  <input> 
        |  <output> 
        |  <basicString> 
        |  <meanString> 
        """.stripMargin)
      System.exit(1)
    }

    val Array(tilePath, output, basicString, meanString) = args
    val basicNames = basicString.split(",")
    val meanNames = meanString.split(",")

    implicit val spark : SparkSession = SparkSession.builder()
      .appName("Combiner")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.fast.upload", "true")
      .getOrCreate()

    implicit val sc : SparkContext = spark.sparkContext

    // val municipalities = ShapeFileReader.readMultiPolygonFeatures("./municipalities/LocalMunicipalities2011.shp")

    val country : MultiPolygon = ShapeFileReader.readMultiPolygonFeatures("./ZAF/ZAF_adm0.shp")(0).geom

    def makeRDD(layerName: String, path: String, country: MultiPolygon) = {
      val inLayerId = LayerId(layerName, 8)
      require(HadoopAttributeStore(path).layerExists(inLayerId))

      HadoopLayerReader(path)
        .query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](inLayerId)
        .where(Intersects(country))
        .result
    }

    def joinSingles(
      first: ContextRDD[SpatialKey, Seq[Tile], TileLayerMetadata[SpatialKey]], 
      second: ContextRDD[SpatialKey, Seq[Tile], TileLayerMetadata[SpatialKey]]) :
        ContextRDD[SpatialKey, Seq[Tile], TileLayerMetadata[SpatialKey]] = {

      val md = first.metadata
      first.spatialJoin(second)
        .withContext{ _.mapValues{ case (t1, t2) => t1 ++ t2 }}
        .mapContext { bounds => md.updateBounds(bounds) }
    }

    def readBasic(names: Seq[String]) = names.map(makeRDD(_, tilePath, country))
    def readMeans(names: Seq[String]) = names.map(makeRDD(_, tilePath, country)).map(r => TileLayerRDD(r, r.metadata).focalMean(Square(2)))

    val basics = readBasic(basicNames)
    val means = readMeans(meanNames)
    val reduced = (basics ++ means)
      .map(rdd => rdd.withContext{ _.mapValues(Seq(_))})
      .reduce(joinSingles)

    // val mapTransform = reduced.metadata.layout.mapTransform
    // reduced.map{ case (k, ts) => (k, ts.map(t => RasterToPoints.fromDouble(t, mapTransform(k)).toList ))}

    val header = basicNames ++ meanNames.map(_ + "_neighborhood")
    val data = reduced
      .map{ case (t1, t2) => t2.map(_.toArrayDouble) }
      .map(_.transpose)
      .flatMap(_.toSeq)
      .filter(_.forall(_ >= 0))
      .collect()

    // write csv
    val writer = CSVWriter.open(new File(output))
    writer.writeRow(header)
    writer.writeAll(data)
    writer.close()

    // GeoTiff(reduced.stitch, reduced.metadata.crs).write("test-mean.tif")
  }
}

