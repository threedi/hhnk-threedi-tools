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
    <rasterrenderer band="1" alphaBand="-1" nodataColor="" opacity="1" type="singlebandpseudocolor" classificationMax="5" classificationMin="-5">
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
        <colorrampshader maximumValue="5" classificationMode="1" clip="0" colorRampType="INTERPOLATED" labelPrecision="6" minimumValue="-5">
          <item alpha="255" color="#050603" label="-5" value="-5"/>
          <item alpha="255" color="#373724" label="-4" value="-4"/>
          <item alpha="255" color="#183e29" label="-3" value="-3"/>
          <item alpha="255" color="#346945" label="-2" value="-2"/>
          <item alpha="255" color="#3e8a59" label="-1" value="-1"/>
          <item alpha="255" color="#6ca363" label="0" value="0"/>
          <item alpha="255" color="#a5ba6f" label="1" value="1"/>
          <item alpha="255" color="#e7d57a" label="2" value="2"/>
          <item alpha="255" color="#c7a75c" label="3" value="3"/>
          <item alpha="255" color="#b0783a" label="4" value="4"/>
          <item alpha="255" color="#d77f3f" label="5" value="5"/>
          <item alpha="255" color="#103f5f" label="10" value="10"/>
        </colorrampshader>
      </rastershader>
    </rasterrenderer>
    <brightnesscontrast brightness="0" gamma="1" contrast="0"/>
    <huesaturation colorizeRed="255" colorizeStrength="100" colorizeGreen="128" colorizeOn="0" saturation="0" grayscaleMode="0" colorizeBlue="128"/>
    <rasterresampler maxOversampling="2"/>
    <resamplingStage>resamplingFilter</resamplingStage>
  </pipe>
  <originalStyle>
    <maplayer autoRefreshEnabled="0" styleCategories="AllStyleCategories" refreshOnNotifyEnabled="0" type="raster" minScale="1e+08" hasScaleBasedVisibilityFlag="0" autoRefreshTime="0" refreshOnNotifyMessage="" maxScale="inf">
      <id>dem_Marken_copy20190520162133118</id>
      <datasource>../02. Sqlite modeldatabase/rasters/dem_waterland.tif</datasource>
      <keywordList>
        <value/>
      </keywordList>
      <layername>dem</layername>
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
        <rasterrenderer band="1" alphaBand="-1" opacity="1" type="singlebandpseudocolor" classificationMax="5" classificationMin="-5">
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
              <item alpha="255" color="#050603" label="-5" value="-5"/>
              <item alpha="255" color="#373724" label="-4" value="-4"/>
              <item alpha="255" color="#183e29" label="-3" value="-3"/>
              <item alpha="255" color="#346945" label="-2" value="-2"/>
              <item alpha="255" color="#3e8a59" label="-1" value="-1"/>
              <item alpha="255" color="#6ca363" label="0" value="0"/>
              <item alpha="255" color="#a5ba6f" label="1" value="1"/>
              <item alpha="255" color="#e7d57a" label="2" value="2"/>
              <item alpha="255" color="#c7a75c" label="3" value="3"/>
              <item alpha="255" color="#b0783a" label="4" value="4"/>
              <item alpha="255" color="#d77f3f" label="5" value="5"/>
              <item alpha="255" color="#103f5f" label="10" value="10"/>
            </colorrampshader>
          </rastershader>
        </rasterrenderer>
        <brightnesscontrast brightness="0" contrast="0"/>
        <huesaturation colorizeRed="255" colorizeStrength="100" colorizeGreen="128" colorizeOn="0" saturation="0" colorizeBlue="128" grayscaleMode="0"/>
        <rasterresampler maxOversampling="2"/>
      </pipe>
      <blendMode>0</blendMode>
    </maplayer>
  </originalStyle>
  <blendMode>0</blendMode>
</qgis>
