package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.model

import org.locationtech.jts.geom.Geometry

case class TaggedGeometry(sourceLayerType: LayerType, geometry: Geometry)
