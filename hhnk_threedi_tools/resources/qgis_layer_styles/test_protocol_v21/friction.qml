<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis styleCategories="AllStyleCategories" hasScaleBasedVisibilityFlag="0" minScale="1e+08" version="3.16.11-Hannover" maxScale="inf">
  <flags>
    <Identifiable>1</Identifiable>
    <Removable>1</Removable>
    <Searchable>1</Searchable>
  </flags>
  <temporal enabled="0" mode="0" fetchMode="0">
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
      <resampling zoomedOutResamplingMethod="nearestNeighbour" enabled="false" zoomedInResamplingMethod="nearestNeighbour" maxOversampling="2"/>
    </provider>
    <rasterrenderer band="1" alphaBand="0" nodataColor="" opacity="1" type="singlebandpseudocolor" classificationMax="0.2" classificationMin="0.067">
      <rasterTransparency/>
      <minMaxOrigin>
        <limits>None</limits>
        <extent>WholeRaster</extent>
        <statAccuracy>Exact</statAccuracy>
        <cumulativeCutLower>0.02</cumulativeCutLower>
        <cumulativeCutUpper>0.98</cumulativeCutUpper>
        <stdDevFactor>2</stdDevFactor>
      </minMaxOrigin>
      <rastershader>
        <colorrampshader maximumValue="0.2" classificationMode="1" clip="0" colorRampType="INTERPOLATED" labelPrecision="6" minimumValue="0.067">
          <item alpha="255" color="#fafafa" label="0.067" value="0.067"/>
          <item alpha="255" color="#dfdfdf" label="0.0818" value="0.0818"/>
          <item alpha="255" color="#c4c4c4" label="0.0966" value="0.0966"/>
          <item alpha="255" color="#a8a8a8" label="0.111" value="0.111"/>
          <item alpha="255" color="#8d8d8d" label="0.126" value="0.126"/>
          <item alpha="255" color="#727272" label="0.141" value="0.141"/>
          <item alpha="255" color="#575757" label="0.156" value="0.156"/>
          <item alpha="255" color="#3b3b3b" label="0.17" value="0.17"/>
          <item alpha="255" color="#202020" label="0.185" value="0.185"/>
          <item alpha="255" color="#050505" label="0.2" value="0.2"/>
        </colorrampshader>
      </rastershader>
    </rasterrenderer>
    <brightnesscontrast brightness="0" gamma="1" contrast="0"/>
    <huesaturation colorizeRed="255" colorizeStrength="100" colorizeGreen="128" colorizeOn="0" saturation="0" grayscaleMode="0" colorizeBlue="128"/>
    <rasterresampler maxOversampling="2" zoomedOutResampler="bilinear" zoomedInResampler="cubic"/>
    <resamplingStage>resamplingFilter</resamplingStage>
  </pipe>
  <originalStyle>
    <maplayer autoRefreshEnabled="0" styleCategories="AllStyleCategories" refreshOnNotifyEnabled="0" type="raster" minScale="1e+08" hasScaleBasedVisibilityFlag="0" autoRefreshTime="0" refreshOnNotifyMessage="" maxScale="inf">
      <id>friction_Marken20190520161435661</id>
      <datasource>../02. Sqlite modeldatabase/rasters/friction_waterland.tif</datasource>
      <keywordList>
        <value/>
      </keywordList>
      <layername>friction</layername>
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
        <noDataList useSrcNoData="1" bandNo="1"/>
      </noData>
      <map-layer-style-manager current="standaard">
        <map-layer-style name="standaard"/>
      </map-layer-style-manager>
      <flags>
        <Identifiable>1</Identifiable>
        <Removable>1</Removable>
        <Searchable>1</Searchable>
      </flags>
      <customproperties>
        <property key="embeddedWidgets/count" value="0"/>
        <property key="identify/format" value="Value"/>
      </customproperties>
      <pipe>
        <rasterrenderer band="1" alphaBand="0" opacity="1" type="singlebandpseudocolor" classificationMax="0.2" classificationMin="0.067">
          <rasterTransparency/>
          <minMaxOrigin>
            <limits>None</limits>
            <extent>WholeRaster</extent>
            <statAccuracy>Exact</statAccuracy>
            <cumulativeCutLower>0.02</cumulativeCutLower>
            <cumulativeCutUpper>0.98</cumulativeCutUpper>
            <stdDevFactor>2</stdDevFactor>
          </minMaxOrigin>
          <rastershader>
            <colorrampshader classificationMode="1" clip="0" colorRampType="INTERPOLATED">
              <item alpha="255" color="#fafafa" label="0.067" value="0.067"/>
              <item alpha="255" color="#dfdfdf" label="0.0818" value="0.0818"/>
              <item alpha="255" color="#c4c4c4" label="0.0966" value="0.0966"/>
              <item alpha="255" color="#a8a8a8" label="0.111" value="0.111"/>
              <item alpha="255" color="#8d8d8d" label="0.126" value="0.126"/>
              <item alpha="255" color="#727272" label="0.141" value="0.141"/>
              <item alpha="255" color="#575757" label="0.156" value="0.156"/>
              <item alpha="255" color="#3b3b3b" label="0.17" value="0.17"/>
              <item alpha="255" color="#202020" label="0.185" value="0.185"/>
              <item alpha="255" color="#050505" label="0.2" value="0.2"/>
            </colorrampshader>
          </rastershader>
        </rasterrenderer>
        <brightnesscontrast brightness="0" contrast="0"/>
        <huesaturation colorizeRed="255" colorizeStrength="100" colorizeGreen="128" colorizeOn="0" saturation="0" colorizeBlue="128" grayscaleMode="0"/>
        <rasterresampler maxOversampling="2" zoomedOutResampler="bilinear" zoomedInResampler="cubic"/>
      </pipe>
      <blendMode>0</blendMode>
    </maplayer>
  </originalStyle>
  <blendMode>0</blendMode>
</qgis>
