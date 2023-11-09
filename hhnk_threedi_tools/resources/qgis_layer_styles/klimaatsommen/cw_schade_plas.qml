<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis maxScale="inf" version="3.16.11-Hannover" hasScaleBasedVisibilityFlag="0" minScale="1e+08" styleCategories="AllStyleCategories">
  <flags>
    <Identifiable>1</Identifiable>
    <Removable>1</Removable>
    <Searchable>0</Searchable>
  </flags>
  <temporal mode="0" enabled="0" fetchMode="0">
    <fixedRange>
      <start></start>
      <end></end>
    </fixedRange>
  </temporal>
  <customproperties>
    <property key="embeddedWidgets/count" value="0"/>
    <property key="identify/format" value="Value"/>
  </customproperties>
  <pipe>
    <provider>
      <resampling enabled="false" zoomedInResamplingMethod="nearestNeighbour" maxOversampling="2" zoomedOutResamplingMethod="nearestNeighbour"/>
    </provider>
    <rasterrenderer classificationMin="10" type="singlebandpseudocolor" alphaBand="-1" opacity="1" nodataColor="" classificationMax="60" band="1">
      <rasterTransparency>
        <singleValuePixelList>
          <pixelListEntry min="0" percentTransparent="100" max="0.1"/>
        </singleValuePixelList>
      </rasterTransparency>
      <minMaxOrigin>
        <limits>None</limits>
        <extent>WholeRaster</extent>
        <statAccuracy>Exact</statAccuracy>
        <cumulativeCutLower>0.02</cumulativeCutLower>
        <cumulativeCutUpper>0.98</cumulativeCutUpper>
        <stdDevFactor>2</stdDevFactor>
      </minMaxOrigin>
      <rastershader>
        <colorrampshader colorRampType="DISCRETE" clip="0" labelPrecision="6" minimumValue="10" classificationMode="1" maximumValue="60">
          <item color="#ffffb2" label="&lt;= 10" alpha="255" value="10"/>
          <item color="#ffd76d" label="10 - 20" alpha="255" value="20"/>
          <item color="#fea649" label="20 - 30" alpha="255" value="30"/>
          <item color="#f86c30" label="30 - 40" alpha="255" value="40"/>
          <item color="#e62f21" label="40 - 50" alpha="255" value="50"/>
          <item color="#bd0026" label="> 50" alpha="255" value="inf"/>
        </colorrampshader>
      </rastershader>
    </rasterrenderer>
    <brightnesscontrast gamma="1" contrast="0" brightness="0"/>
    <huesaturation colorizeGreen="128" colorizeOn="0" colorizeBlue="128" colorizeStrength="100" colorizeRed="255" saturation="0" grayscaleMode="0"/>
    <rasterresampler maxOversampling="2"/>
    <resamplingStage>resamplingFilter</resamplingStage>
  </pipe>
  <originalStyle>
    <maplayer maxScale="inf" refreshOnNotifyEnabled="0" type="raster" hasScaleBasedVisibilityFlag="0" autoRefreshEnabled="0" autoRefreshTime="0" minScale="1e+08" styleCategories="AllStyleCategories" refreshOnNotifyMessage="">
      <extent>
        <xmin>113647</xmin>
        <ymin>503701</ymin>
        <xmax>117418</xmax>
        <ymax>507882</ymax>
      </extent>
      <id>cw_schade_plas_8e574c26_41a8_4bf0_96f6_11cc241436ed</id>
      <datasource>./02_output_rasters/cw_schade_plas.tif</datasource>
      <keywordList>
        <value/>
      </keywordList>
      <layername>cw_schade_plas</layername>
      <srs>
        <spatialrefsys>
          <proj4>+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +towgs84=565.2369,50.0087,465.658,-0.406857,0.350733,-1.87035,4.0812 +units=m +no_defs</proj4>
          <srsid>2517</srsid>
          <srid>28992</srid>
          <authid>EPSG:28992</authid>
          <description>Amersfoort / RD New</description>
          <projectionacronym>sterea</projectionacronym>
          <ellipsoidacronym>bessel</ellipsoidacronym>
          <geographicflag>false</geographicflag>
        </spatialrefsys>
      </srs>
      <resourceMetadata>
        <identifier/>
        <parentidentifier/>
        <language/>
        <type/>
        <title/>
        <abstract/>
        <links/>
        <fees/>
        <encoding/>
        <crs>
          <spatialrefsys>
            <proj4/>
            <srsid>0</srsid>
            <srid>0</srid>
            <authid/>
            <description/>
            <projectionacronym/>
            <ellipsoidacronym/>
            <geographicflag>true</geographicflag>
          </spatialrefsys>
        </crs>
        <extent/>
      </resourceMetadata>
      <provider>gdal</provider>
      <noData>
        <noDataList bandNo="1" useSrcNoData="1"/>
      </noData>
      <map-layer-style-manager current="default">
        <map-layer-style name="default"/>
      </map-layer-style-manager>
      <flags>
        <Identifiable>1</Identifiable>
        <Removable>1</Removable>
        <Searchable>0</Searchable>
      </flags>
      <customproperties>
        <property key="embeddedWidgets/count" value="0"/>
        <property key="identify/format" value="Value"/>
      </customproperties>
      <pipe>
        <rasterrenderer classificationMin="10" alphaBand="-1" type="singlebandpseudocolor" opacity="1" classificationMax="60" band="1">
          <rasterTransparency>
            <singleValuePixelList>
              <pixelListEntry min="0" percentTransparent="100" max="0.1"/>
            </singleValuePixelList>
          </rasterTransparency>
          <minMaxOrigin>
            <limits>None</limits>
            <extent>WholeRaster</extent>
            <statAccuracy>Exact</statAccuracy>
            <cumulativeCutLower>0.02</cumulativeCutLower>
            <cumulativeCutUpper>0.98</cumulativeCutUpper>
            <stdDevFactor>2</stdDevFactor>
          </minMaxOrigin>
          <rastershader>
            <colorrampshader colorRampType="DISCRETE" clip="0" classificationMode="1">
              <item color="#ffffb2" label="&lt;= 10" alpha="255" value="10"/>
              <item color="#ffd76d" label="10 - 20" alpha="255" value="20"/>
              <item color="#fea649" label="20 - 30" alpha="255" value="30"/>
              <item color="#f86c30" label="30 - 40" alpha="255" value="40"/>
              <item color="#e62f21" label="40 - 50" alpha="255" value="50"/>
              <item color="#bd0026" label="> 50" alpha="255" value="inf"/>
            </colorrampshader>
          </rastershader>
        </rasterrenderer>
        <brightnesscontrast contrast="0" brightness="0"/>
        <huesaturation colorizeGreen="128" colorizeOn="0" colorizeBlue="128" colorizeStrength="100" colorizeRed="255" saturation="0" grayscaleMode="0"/>
        <rasterresampler maxOversampling="2"/>
      </pipe>
      <blendMode>0</blendMode>
    </maplayer>
  </originalStyle>
  <blendMode>0</blendMode>
</qgis>
