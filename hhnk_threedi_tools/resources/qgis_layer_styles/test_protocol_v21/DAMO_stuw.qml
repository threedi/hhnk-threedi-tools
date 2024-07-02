<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis hasScaleBasedVisibilityFlag="1" version="3.22.7-Białowieża" symbologyReferenceScale="-1" maxScale="1" labelsEnabled="0" simplifyDrawingHints="0" simplifyMaxScale="1" simplifyAlgorithm="0" simplifyDrawingTol="1" styleCategories="AllStyleCategories" readOnly="0" simplifyLocal="1" minScale="25000">
  <flags>
    <Identifiable>1</Identifiable>
    <Removable>1</Removable>
    <Searchable>1</Searchable>
    <Private>0</Private>
  </flags>
  <temporal durationUnit="min" startField="" durationField="" accumulate="0" endField="" fixedDuration="0" enabled="0" startExpression="" mode="0" limitMode="0" endExpression="">
    <fixedRange>
      <start></start>
      <end></end>
    </fixedRange>
  </temporal>
  <renderer-v2 forceraster="0" type="RuleRenderer" enableorderby="0" referencescale="-1" symbollevels="0">
    <rules key="{a0ba2603-be68-498f-bb2b-0ac962b9e640}">
      <rule symbol="0" scalemaxdenom="25000" filter="concat(&quot;WS_CATEGORIE&quot;,',',&quot;SOORTREGELBAARHEID&quot;) = '2,1'" scalemindenom="1" label="Secundair, vast" key="{c5fa7c39-73dc-42b0-8624-b3b0b5452bff}"/>
      <rule symbol="1" scalemaxdenom="25000" filter="concat(&quot;WS_CATEGORIE&quot;,',',&quot;SOORTREGELBAARHEID&quot;) = '2,&lt;Null>'" scalemindenom="1" label="Secundair, vast" key="{cc1000b8-cb52-4521-a8a4-417dde33885f}"/>
      <rule symbol="2" scalemaxdenom="25000" filter="concat(&quot;WS_CATEGORIE&quot;,',',&quot;SOORTREGELBAARHEID&quot;) = '2,2'" scalemindenom="1" label="Secundair, afsluitbaar" key="{ac7dc44d-fd90-4006-9f44-5f452ed30422}"/>
      <rule symbol="3" scalemaxdenom="25000" filter="concat(&quot;WS_CATEGORIE&quot;,',',&quot;SOORTREGELBAARHEID&quot;) = '2,4'" scalemindenom="1" label="Secundair, afsluitbaar" key="{c617e82f-4163-4211-a7ac-382f4e6128aa}"/>
      <rule symbol="4" scalemaxdenom="25000" filter="concat(&quot;WS_CATEGORIE&quot;,',',&quot;SOORTREGELBAARHEID&quot;) = '2,3'" scalemindenom="1" label="Secundair, afsluitbaar" key="{febb183d-3496-44f7-a67b-270e50efb7fa}"/>
      <rule symbol="5" scalemaxdenom="50000" filter="concat(&quot;WS_CATEGORIE&quot;,',',&quot;SOORTREGELBAARHEID&quot;) = '1,1'" scalemindenom="1" label="Primair, vast" key="{87eeda13-843f-46cb-b120-41d0fedd080b}"/>
      <rule symbol="6" scalemaxdenom="50000" filter="concat(&quot;WS_CATEGORIE&quot;,',',&quot;SOORTREGELBAARHEID&quot;) = '1,2'" scalemindenom="1" label="Primair, niet automatisch" key="{daebca81-d3cf-4225-8847-4e0b92e20823}"/>
      <rule symbol="7" scalemaxdenom="50000" filter="concat(&quot;WS_CATEGORIE&quot;,',',&quot;SOORTREGELBAARHEID&quot;) = '1,4'" scalemindenom="1" label="Primair, niet automatisch" key="{17076ee4-0181-41c8-9000-07b0bbf31f85}"/>
      <rule symbol="8" scalemaxdenom="50000" filter="concat(&quot;WS_CATEGORIE&quot;,',',&quot;SOORTREGELBAARHEID&quot;) = '1,3'" scalemindenom="1" label="Primair, automatisch" key="{39b52f76-9a24-4b95-8769-9d7ccd64735c}"/>
      <rule symbol="9" filter="ELSE" label="anders" key="{ecd0f398-b4a9-4a8b-9edd-c67bbf926a38}"/>
    </rules>
    <symbols>
      <symbol alpha="1" type="marker" name="0" force_rhr="0" clip_to_extent="1">
        <data_defined_properties>
          <Option type="Map">
            <Option type="QString" name="name" value=""/>
            <Option name="properties"/>
            <Option type="QString" name="type" value="collection"/>
          </Option>
        </data_defined_properties>
        <layer locked="0" class="RasterMarker" enabled="1" pass="0">
          <Option type="Map">
            <Option type="QString" name="alpha" value="1"/>
            <Option type="QString" name="angle" value="0"/>
            <Option type="QString" name="fixedAspectRatio" value="0.0833333"/>
            <Option type="QString" name="horizontal_anchor_point" value="1"/>
            <Option type="QString" name="imageFile" value="base64:iVBORw0KGgoAAAANSUhEUgAAABAAAAACCAYAAABR7VzxAAAAAXNSR0IB2cksfwAAAAlwSFlzAAAOxAAADsQBlSsOGwAAAChJREFUCJljYYCATQwMDEoMpIHTDAwMiSxQjhIDA4MGiQa8ZGBgYAAAzPkDPMUR6EUAAAAASUVORK5CYII="/>
            <Option type="QString" name="offset" value="0,0"/>
            <Option type="QString" name="offset_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="offset_unit" value="Point"/>
            <Option type="QString" name="scale_method" value="diameter"/>
            <Option type="QString" name="size" value="12"/>
            <Option type="QString" name="size_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="size_unit" value="Point"/>
            <Option type="QString" name="vertical_anchor_point" value="1"/>
          </Option>
          <prop k="alpha" v="1"/>
          <prop k="angle" v="0"/>
          <prop k="fixedAspectRatio" v="0.0833333"/>
          <prop k="horizontal_anchor_point" v="1"/>
          <prop k="imageFile" v="base64:iVBORw0KGgoAAAANSUhEUgAAABAAAAACCAYAAABR7VzxAAAAAXNSR0IB2cksfwAAAAlwSFlzAAAOxAAADsQBlSsOGwAAAChJREFUCJljYYCATQwMDEoMpIHTDAwMiSxQjhIDA4MGiQa8ZGBgYAAAzPkDPMUR6EUAAAAASUVORK5CYII="/>
          <prop k="offset" v="0,0"/>
          <prop k="offset_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="offset_unit" v="Point"/>
          <prop k="scale_method" v="diameter"/>
          <prop k="size" v="12"/>
          <prop k="size_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="size_unit" v="Point"/>
          <prop k="vertical_anchor_point" v="1"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol alpha="1" type="marker" name="1" force_rhr="0" clip_to_extent="1">
        <data_defined_properties>
          <Option type="Map">
            <Option type="QString" name="name" value=""/>
            <Option name="properties"/>
            <Option type="QString" name="type" value="collection"/>
          </Option>
        </data_defined_properties>
        <layer locked="0" class="RasterMarker" enabled="1" pass="0">
          <Option type="Map">
            <Option type="QString" name="alpha" value="1"/>
            <Option type="QString" name="angle" value="0"/>
            <Option type="QString" name="fixedAspectRatio" value="0.0833333"/>
            <Option type="QString" name="horizontal_anchor_point" value="1"/>
            <Option type="QString" name="imageFile" value="base64:iVBORw0KGgoAAAANSUhEUgAAABAAAAACCAYAAABR7VzxAAAAAXNSR0IB2cksfwAAAAlwSFlzAAAOxAAADsQBlSsOGwAAAChJREFUCJljYYCATQwMDEoMpIHTDAwMiSxQjhIDA4MGiQa8ZGBgYAAAzPkDPMUR6EUAAAAASUVORK5CYII="/>
            <Option type="QString" name="offset" value="0,0"/>
            <Option type="QString" name="offset_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="offset_unit" value="Point"/>
            <Option type="QString" name="scale_method" value="diameter"/>
            <Option type="QString" name="size" value="12"/>
            <Option type="QString" name="size_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="size_unit" value="Point"/>
            <Option type="QString" name="vertical_anchor_point" value="1"/>
          </Option>
          <prop k="alpha" v="1"/>
          <prop k="angle" v="0"/>
          <prop k="fixedAspectRatio" v="0.0833333"/>
          <prop k="horizontal_anchor_point" v="1"/>
          <prop k="imageFile" v="base64:iVBORw0KGgoAAAANSUhEUgAAABAAAAACCAYAAABR7VzxAAAAAXNSR0IB2cksfwAAAAlwSFlzAAAOxAAADsQBlSsOGwAAAChJREFUCJljYYCATQwMDEoMpIHTDAwMiSxQjhIDA4MGiQa8ZGBgYAAAzPkDPMUR6EUAAAAASUVORK5CYII="/>
          <prop k="offset" v="0,0"/>
          <prop k="offset_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="offset_unit" v="Point"/>
          <prop k="scale_method" v="diameter"/>
          <prop k="size" v="12"/>
          <prop k="size_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="size_unit" v="Point"/>
          <prop k="vertical_anchor_point" v="1"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol alpha="1" type="marker" name="2" force_rhr="0" clip_to_extent="1">
        <data_defined_properties>
          <Option type="Map">
            <Option type="QString" name="name" value=""/>
            <Option name="properties"/>
            <Option type="QString" name="type" value="collection"/>
          </Option>
        </data_defined_properties>
        <layer locked="0" class="RasterMarker" enabled="1" pass="0">
          <Option type="Map">
            <Option type="QString" name="alpha" value="1"/>
            <Option type="QString" name="angle" value="0"/>
            <Option type="QString" name="fixedAspectRatio" value="0.333333"/>
            <Option type="QString" name="horizontal_anchor_point" value="1"/>
            <Option type="QString" name="imageFile" value="base64:iVBORw0KGgoAAAANSUhEUgAAABAAAAAGCAYAAADKfB7nAAAAAXNSR0IB2cksfwAAAAlwSFlzAAAOxAAADsQBlSsOGwAAAGpJREFUGJWVj6ENwzAURB+wdFIsBVgG3aBrBHWIqEM12cijFBkEVUHHSpzwf+yBe7pLAJJetmcCkXTYbmnwDjwjAtsNWNKAD1AjAuALcC2owCMoOG+BpLft0AWgAVsCyDmvwBRpl1J+vXf+AZgbg1xN2rcAAAAASUVORK5CYII="/>
            <Option type="QString" name="offset" value="0,0"/>
            <Option type="QString" name="offset_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="offset_unit" value="Point"/>
            <Option type="QString" name="scale_method" value="diameter"/>
            <Option type="QString" name="size" value="12"/>
            <Option type="QString" name="size_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="size_unit" value="Point"/>
            <Option type="QString" name="vertical_anchor_point" value="1"/>
          </Option>
          <prop k="alpha" v="1"/>
          <prop k="angle" v="0"/>
          <prop k="fixedAspectRatio" v="0.333333"/>
          <prop k="horizontal_anchor_point" v="1"/>
          <prop k="imageFile" v="base64:iVBORw0KGgoAAAANSUhEUgAAABAAAAAGCAYAAADKfB7nAAAAAXNSR0IB2cksfwAAAAlwSFlzAAAOxAAADsQBlSsOGwAAAGpJREFUGJWVj6ENwzAURB+wdFIsBVgG3aBrBHWIqEM12cijFBkEVUHHSpzwf+yBe7pLAJJetmcCkXTYbmnwDjwjAtsNWNKAD1AjAuALcC2owCMoOG+BpLft0AWgAVsCyDmvwBRpl1J+vXf+AZgbg1xN2rcAAAAASUVORK5CYII="/>
          <prop k="offset" v="0,0"/>
          <prop k="offset_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="offset_unit" v="Point"/>
          <prop k="scale_method" v="diameter"/>
          <prop k="size" v="12"/>
          <prop k="size_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="size_unit" v="Point"/>
          <prop k="vertical_anchor_point" v="1"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol alpha="1" type="marker" name="3" force_rhr="0" clip_to_extent="1">
        <data_defined_properties>
          <Option type="Map">
            <Option type="QString" name="name" value=""/>
            <Option name="properties"/>
            <Option type="QString" name="type" value="collection"/>
          </Option>
        </data_defined_properties>
        <layer locked="0" class="RasterMarker" enabled="1" pass="0">
          <Option type="Map">
            <Option type="QString" name="alpha" value="1"/>
            <Option type="QString" name="angle" value="0"/>
            <Option type="QString" name="fixedAspectRatio" value="0.333333"/>
            <Option type="QString" name="horizontal_anchor_point" value="1"/>
            <Option type="QString" name="imageFile" value="base64:iVBORw0KGgoAAAANSUhEUgAAABAAAAAGCAYAAADKfB7nAAAAAXNSR0IB2cksfwAAAAlwSFlzAAAOxAAADsQBlSsOGwAAAGpJREFUGJWVj6ENwzAURB+wdFIsBVgG3aBrBHWIqEM12cijFBkEVUHHSpzwf+yBe7pLAJJetmcCkXTYbmnwDjwjAtsNWNKAD1AjAuALcC2owCMoOG+BpLft0AWgAVsCyDmvwBRpl1J+vXf+AZgbg1xN2rcAAAAASUVORK5CYII="/>
            <Option type="QString" name="offset" value="0,0"/>
            <Option type="QString" name="offset_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="offset_unit" value="Point"/>
            <Option type="QString" name="scale_method" value="diameter"/>
            <Option type="QString" name="size" value="12"/>
            <Option type="QString" name="size_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="size_unit" value="Point"/>
            <Option type="QString" name="vertical_anchor_point" value="1"/>
          </Option>
          <prop k="alpha" v="1"/>
          <prop k="angle" v="0"/>
          <prop k="fixedAspectRatio" v="0.333333"/>
          <prop k="horizontal_anchor_point" v="1"/>
          <prop k="imageFile" v="base64:iVBORw0KGgoAAAANSUhEUgAAABAAAAAGCAYAAADKfB7nAAAAAXNSR0IB2cksfwAAAAlwSFlzAAAOxAAADsQBlSsOGwAAAGpJREFUGJWVj6ENwzAURB+wdFIsBVgG3aBrBHWIqEM12cijFBkEVUHHSpzwf+yBe7pLAJJetmcCkXTYbmnwDjwjAtsNWNKAD1AjAuALcC2owCMoOG+BpLft0AWgAVsCyDmvwBRpl1J+vXf+AZgbg1xN2rcAAAAASUVORK5CYII="/>
          <prop k="offset" v="0,0"/>
          <prop k="offset_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="offset_unit" v="Point"/>
          <prop k="scale_method" v="diameter"/>
          <prop k="size" v="12"/>
          <prop k="size_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="size_unit" v="Point"/>
          <prop k="vertical_anchor_point" v="1"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol alpha="1" type="marker" name="4" force_rhr="0" clip_to_extent="1">
        <data_defined_properties>
          <Option type="Map">
            <Option type="QString" name="name" value=""/>
            <Option name="properties"/>
            <Option type="QString" name="type" value="collection"/>
          </Option>
        </data_defined_properties>
        <layer locked="0" class="RasterMarker" enabled="1" pass="0">
          <Option type="Map">
            <Option type="QString" name="alpha" value="1"/>
            <Option type="QString" name="angle" value="0"/>
            <Option type="QString" name="fixedAspectRatio" value="0.333333"/>
            <Option type="QString" name="horizontal_anchor_point" value="1"/>
            <Option type="QString" name="imageFile" value="base64:iVBORw0KGgoAAAANSUhEUgAAABAAAAAGCAYAAADKfB7nAAAAAXNSR0IB2cksfwAAAAlwSFlzAAAOxAAADsQBlSsOGwAAAGpJREFUGJWVj6ENwzAURB+wdFIsBVgG3aBrBHWIqEM12cijFBkEVUHHSpzwf+yBe7pLAJJetmcCkXTYbmnwDjwjAtsNWNKAD1AjAuALcC2owCMoOG+BpLft0AWgAVsCyDmvwBRpl1J+vXf+AZgbg1xN2rcAAAAASUVORK5CYII="/>
            <Option type="QString" name="offset" value="0,0"/>
            <Option type="QString" name="offset_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="offset_unit" value="Point"/>
            <Option type="QString" name="scale_method" value="diameter"/>
            <Option type="QString" name="size" value="12"/>
            <Option type="QString" name="size_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="size_unit" value="Point"/>
            <Option type="QString" name="vertical_anchor_point" value="1"/>
          </Option>
          <prop k="alpha" v="1"/>
          <prop k="angle" v="0"/>
          <prop k="fixedAspectRatio" v="0.333333"/>
          <prop k="horizontal_anchor_point" v="1"/>
          <prop k="imageFile" v="base64:iVBORw0KGgoAAAANSUhEUgAAABAAAAAGCAYAAADKfB7nAAAAAXNSR0IB2cksfwAAAAlwSFlzAAAOxAAADsQBlSsOGwAAAGpJREFUGJWVj6ENwzAURB+wdFIsBVgG3aBrBHWIqEM12cijFBkEVUHHSpzwf+yBe7pLAJJetmcCkXTYbmnwDjwjAtsNWNKAD1AjAuALcC2owCMoOG+BpLft0AWgAVsCyDmvwBRpl1J+vXf+AZgbg1xN2rcAAAAASUVORK5CYII="/>
          <prop k="offset" v="0,0"/>
          <prop k="offset_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="offset_unit" v="Point"/>
          <prop k="scale_method" v="diameter"/>
          <prop k="size" v="12"/>
          <prop k="size_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="size_unit" v="Point"/>
          <prop k="vertical_anchor_point" v="1"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol alpha="1" type="marker" name="5" force_rhr="0" clip_to_extent="1">
        <data_defined_properties>
          <Option type="Map">
            <Option type="QString" name="name" value=""/>
            <Option name="properties"/>
            <Option type="QString" name="type" value="collection"/>
          </Option>
        </data_defined_properties>
        <layer locked="0" class="RasterMarker" enabled="1" pass="0">
          <Option type="Map">
            <Option type="QString" name="alpha" value="1"/>
            <Option type="QString" name="angle" value="0"/>
            <Option type="QString" name="fixedAspectRatio" value="0.294118"/>
            <Option type="QString" name="horizontal_anchor_point" value="1"/>
            <Option type="QString" name="imageFile" value="base64:iVBORw0KGgoAAAANSUhEUgAAABcAAAAHCAYAAADj/NY7AAAAAXNSR0IB2cksfwAAAAlwSFlzAAAOxAAADsQBlSsOGwAAAMRJREFUKJGdziFKRFEUBuAPHLiCIMMFw4uuwWDR2YDFYBJcgztwBwbbgFgMoskpGtyDmMRkErlBvNrkmCwzMsob5unfzvnhO6enPQvYRZrRfyfnfFdrvW3rei27hHNsz4Oh1vqBHVzPw5dwiTXs47ODv44R9nAxC+/jCqtN0wxKKfddPscwpfQUEWdYxvFvfAU34wObpZTHjjCIiAO8Yjg+cDiNb6WUFiNiA89/gadyhDec/MBzzg+11gFe/glPcor3yfAFJecx1NXHzQkAAAAASUVORK5CYII="/>
            <Option type="QString" name="offset" value="0,0"/>
            <Option type="QString" name="offset_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="offset_unit" value="Point"/>
            <Option type="QString" name="scale_method" value="diameter"/>
            <Option type="QString" name="size" value="17"/>
            <Option type="QString" name="size_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="size_unit" value="Point"/>
            <Option type="QString" name="vertical_anchor_point" value="1"/>
          </Option>
          <prop k="alpha" v="1"/>
          <prop k="angle" v="0"/>
          <prop k="fixedAspectRatio" v="0.294118"/>
          <prop k="horizontal_anchor_point" v="1"/>
          <prop k="imageFile" v="base64:iVBORw0KGgoAAAANSUhEUgAAABcAAAAHCAYAAADj/NY7AAAAAXNSR0IB2cksfwAAAAlwSFlzAAAOxAAADsQBlSsOGwAAAMRJREFUKJGdziFKRFEUBuAPHLiCIMMFw4uuwWDR2YDFYBJcgztwBwbbgFgMoskpGtyDmMRkErlBvNrkmCwzMsob5unfzvnhO6enPQvYRZrRfyfnfFdrvW3rei27hHNsz4Oh1vqBHVzPw5dwiTXs47ODv44R9nAxC+/jCqtN0wxKKfddPscwpfQUEWdYxvFvfAU34wObpZTHjjCIiAO8Yjg+cDiNb6WUFiNiA89/gadyhDec/MBzzg+11gFe/glPcor3yfAFJecx1NXHzQkAAAAASUVORK5CYII="/>
          <prop k="offset" v="0,0"/>
          <prop k="offset_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="offset_unit" v="Point"/>
          <prop k="scale_method" v="diameter"/>
          <prop k="size" v="17"/>
          <prop k="size_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="size_unit" v="Point"/>
          <prop k="vertical_anchor_point" v="1"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol alpha="1" type="marker" name="6" force_rhr="0" clip_to_extent="1">
        <data_defined_properties>
          <Option type="Map">
            <Option type="QString" name="name" value=""/>
            <Option name="properties"/>
            <Option type="QString" name="type" value="collection"/>
          </Option>
        </data_defined_properties>
        <layer locked="0" class="RasterMarker" enabled="1" pass="0">
          <Option type="Map">
            <Option type="QString" name="alpha" value="1"/>
            <Option type="QString" name="angle" value="0"/>
            <Option type="QString" name="fixedAspectRatio" value="0.294118"/>
            <Option type="QString" name="horizontal_anchor_point" value="1"/>
            <Option type="QString" name="imageFile" value="base64:iVBORw0KGgoAAAANSUhEUgAAABcAAAAHCAYAAADj/NY7AAAAAXNSR0IB2cksfwAAAAlwSFlzAAAOxAAADsQBlSsOGwAAAMFJREFUKJGd0CFLQwEQwPGfbHADsbxmEmyrAz+DTKvN7DewWU3WFU0iLBhtluWBxSBiVVSwyMPyhGsLPkXGG3vsH+/gx3FdCyqKYqeqqvVF+98y8xGfTbtuw2wtIs7KsjxeBte9YB9Py/Aexpm5ixN8t8D3MI2Ig8ycNOEb2MQV+jjEfcvLb3CRmbc4wgTv//EhrvGB04gYYNASl5lTvOISW/OXb/v52RBvmdnWne8Bz+j84RFxl5nn+FpVrRupXwIzew03Ja+yAwQAAAAASUVORK5CYII="/>
            <Option type="QString" name="offset" value="0,0"/>
            <Option type="QString" name="offset_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="offset_unit" value="Point"/>
            <Option type="QString" name="scale_method" value="diameter"/>
            <Option type="QString" name="size" value="17"/>
            <Option type="QString" name="size_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="size_unit" value="Point"/>
            <Option type="QString" name="vertical_anchor_point" value="1"/>
          </Option>
          <prop k="alpha" v="1"/>
          <prop k="angle" v="0"/>
          <prop k="fixedAspectRatio" v="0.294118"/>
          <prop k="horizontal_anchor_point" v="1"/>
          <prop k="imageFile" v="base64:iVBORw0KGgoAAAANSUhEUgAAABcAAAAHCAYAAADj/NY7AAAAAXNSR0IB2cksfwAAAAlwSFlzAAAOxAAADsQBlSsOGwAAAMFJREFUKJGd0CFLQwEQwPGfbHADsbxmEmyrAz+DTKvN7DewWU3WFU0iLBhtluWBxSBiVVSwyMPyhGsLPkXGG3vsH+/gx3FdCyqKYqeqqvVF+98y8xGfTbtuw2wtIs7KsjxeBte9YB9Py/Aexpm5ixN8t8D3MI2Ig8ycNOEb2MQV+jjEfcvLb3CRmbc4wgTv//EhrvGB04gYYNASl5lTvOISW/OXb/v52RBvmdnWne8Bz+j84RFxl5nn+FpVrRupXwIzew03Ja+yAwQAAAAASUVORK5CYII="/>
          <prop k="offset" v="0,0"/>
          <prop k="offset_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="offset_unit" v="Point"/>
          <prop k="scale_method" v="diameter"/>
          <prop k="size" v="17"/>
          <prop k="size_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="size_unit" v="Point"/>
          <prop k="vertical_anchor_point" v="1"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol alpha="1" type="marker" name="7" force_rhr="0" clip_to_extent="1">
        <data_defined_properties>
          <Option type="Map">
            <Option type="QString" name="name" value=""/>
            <Option name="properties"/>
            <Option type="QString" name="type" value="collection"/>
          </Option>
        </data_defined_properties>
        <layer locked="0" class="RasterMarker" enabled="1" pass="0">
          <Option type="Map">
            <Option type="QString" name="alpha" value="1"/>
            <Option type="QString" name="angle" value="0"/>
            <Option type="QString" name="fixedAspectRatio" value="0.294118"/>
            <Option type="QString" name="horizontal_anchor_point" value="1"/>
            <Option type="QString" name="imageFile" value="base64:iVBORw0KGgoAAAANSUhEUgAAABcAAAAHCAYAAADj/NY7AAAAAXNSR0IB2cksfwAAAAlwSFlzAAAOxAAADsQBlSsOGwAAAMFJREFUKJGd0CFLQwEQwPGfbHADsbxmEmyrAz+DTKvN7DewWU3WFU0iLBhtluWBxSBiVVSwyMPyhGsLPkXGG3vsH+/gx3FdCyqKYqeqqvVF+98y8xGfTbtuw2wtIs7KsjxeBte9YB9Py/Aexpm5ixN8t8D3MI2Ig8ycNOEb2MQV+jjEfcvLb3CRmbc4wgTv//EhrvGB04gYYNASl5lTvOISW/OXb/v52RBvmdnWne8Bz+j84RFxl5nn+FpVrRupXwIzew03Ja+yAwQAAAAASUVORK5CYII="/>
            <Option type="QString" name="offset" value="0,0"/>
            <Option type="QString" name="offset_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="offset_unit" value="Point"/>
            <Option type="QString" name="scale_method" value="diameter"/>
            <Option type="QString" name="size" value="17"/>
            <Option type="QString" name="size_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="size_unit" value="Point"/>
            <Option type="QString" name="vertical_anchor_point" value="1"/>
          </Option>
          <prop k="alpha" v="1"/>
          <prop k="angle" v="0"/>
          <prop k="fixedAspectRatio" v="0.294118"/>
          <prop k="horizontal_anchor_point" v="1"/>
          <prop k="imageFile" v="base64:iVBORw0KGgoAAAANSUhEUgAAABcAAAAHCAYAAADj/NY7AAAAAXNSR0IB2cksfwAAAAlwSFlzAAAOxAAADsQBlSsOGwAAAMFJREFUKJGd0CFLQwEQwPGfbHADsbxmEmyrAz+DTKvN7DewWU3WFU0iLBhtluWBxSBiVVSwyMPyhGsLPkXGG3vsH+/gx3FdCyqKYqeqqvVF+98y8xGfTbtuw2wtIs7KsjxeBte9YB9Py/Aexpm5ixN8t8D3MI2Ig8ycNOEb2MQV+jjEfcvLb3CRmbc4wgTv//EhrvGB04gYYNASl5lTvOISW/OXb/v52RBvmdnWne8Bz+j84RFxl5nn+FpVrRupXwIzew03Ja+yAwQAAAAASUVORK5CYII="/>
          <prop k="offset" v="0,0"/>
          <prop k="offset_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="offset_unit" v="Point"/>
          <prop k="scale_method" v="diameter"/>
          <prop k="size" v="17"/>
          <prop k="size_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="size_unit" v="Point"/>
          <prop k="vertical_anchor_point" v="1"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol alpha="1" type="marker" name="8" force_rhr="0" clip_to_extent="1">
        <data_defined_properties>
          <Option type="Map">
            <Option type="QString" name="name" value=""/>
            <Option name="properties"/>
            <Option type="QString" name="type" value="collection"/>
          </Option>
        </data_defined_properties>
        <layer locked="0" class="RasterMarker" enabled="1" pass="0">
          <Option type="Map">
            <Option type="QString" name="alpha" value="1"/>
            <Option type="QString" name="angle" value="0"/>
            <Option type="QString" name="fixedAspectRatio" value="0.294118"/>
            <Option type="QString" name="horizontal_anchor_point" value="1"/>
            <Option type="QString" name="imageFile" value="base64:iVBORw0KGgoAAAANSUhEUgAAABcAAAAHCAYAAADj/NY7AAAAAXNSR0IB2cksfwAAAAlwSFlzAAAOxAAADsQBlSsOGwAAALVJREFUKJGd0L0uhGEQBtBTbDJ+EhIJrbgNrWoTDZ1GQaOzl0I0q1HIBhFRKlwAN6MiWzydxssmPvHxdDOTOZnMQHcWcYLVH+azmeCuazDo6C3jAZs9YNjGAS5/w1fwWFULSTbw0gPfxwXmcf4Nr6qtJIfYgyRXVTXqebkkU4wxrqrTJKNPPMl6g3GLtyRzfXFcf+wvJTnGF441PGGI1z+gsznCLm5ao73lOckZpv+EW+6x04p3cZYypYQRlEYAAAAASUVORK5CYII="/>
            <Option type="QString" name="offset" value="0,0"/>
            <Option type="QString" name="offset_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="offset_unit" value="Point"/>
            <Option type="QString" name="scale_method" value="diameter"/>
            <Option type="QString" name="size" value="17"/>
            <Option type="QString" name="size_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="size_unit" value="Point"/>
            <Option type="QString" name="vertical_anchor_point" value="1"/>
          </Option>
          <prop k="alpha" v="1"/>
          <prop k="angle" v="0"/>
          <prop k="fixedAspectRatio" v="0.294118"/>
          <prop k="horizontal_anchor_point" v="1"/>
          <prop k="imageFile" v="base64:iVBORw0KGgoAAAANSUhEUgAAABcAAAAHCAYAAADj/NY7AAAAAXNSR0IB2cksfwAAAAlwSFlzAAAOxAAADsQBlSsOGwAAALVJREFUKJGd0L0uhGEQBtBTbDJ+EhIJrbgNrWoTDZ1GQaOzl0I0q1HIBhFRKlwAN6MiWzydxssmPvHxdDOTOZnMQHcWcYLVH+azmeCuazDo6C3jAZs9YNjGAS5/w1fwWFULSTbw0gPfxwXmcf4Nr6qtJIfYgyRXVTXqebkkU4wxrqrTJKNPPMl6g3GLtyRzfXFcf+wvJTnGF441PGGI1z+gsznCLm5ao73lOckZpv+EW+6x04p3cZYypYQRlEYAAAAASUVORK5CYII="/>
          <prop k="offset" v="0,0"/>
          <prop k="offset_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="offset_unit" v="Point"/>
          <prop k="scale_method" v="diameter"/>
          <prop k="size" v="17"/>
          <prop k="size_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="size_unit" v="Point"/>
          <prop k="vertical_anchor_point" v="1"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol alpha="1" type="marker" name="9" force_rhr="0" clip_to_extent="1">
        <data_defined_properties>
          <Option type="Map">
            <Option type="QString" name="name" value=""/>
            <Option name="properties"/>
            <Option type="QString" name="type" value="collection"/>
          </Option>
        </data_defined_properties>
        <layer locked="0" class="SimpleMarker" enabled="1" pass="0">
          <Option type="Map">
            <Option type="QString" name="angle" value="0"/>
            <Option type="QString" name="cap_style" value="square"/>
            <Option type="QString" name="color" value="127,136,165,255"/>
            <Option type="QString" name="horizontal_anchor_point" value="1"/>
            <Option type="QString" name="joinstyle" value="bevel"/>
            <Option type="QString" name="name" value="triangle"/>
            <Option type="QString" name="offset" value="0,0"/>
            <Option type="QString" name="offset_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="offset_unit" value="MM"/>
            <Option type="QString" name="outline_color" value="255,255,255,255"/>
            <Option type="QString" name="outline_style" value="solid"/>
            <Option type="QString" name="outline_width" value="0.4"/>
            <Option type="QString" name="outline_width_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="outline_width_unit" value="MM"/>
            <Option type="QString" name="scale_method" value="area"/>
            <Option type="QString" name="size" value="2.8"/>
            <Option type="QString" name="size_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="size_unit" value="MM"/>
            <Option type="QString" name="vertical_anchor_point" value="1"/>
          </Option>
          <prop k="angle" v="0"/>
          <prop k="cap_style" v="square"/>
          <prop k="color" v="127,136,165,255"/>
          <prop k="horizontal_anchor_point" v="1"/>
          <prop k="joinstyle" v="bevel"/>
          <prop k="name" v="triangle"/>
          <prop k="offset" v="0,0"/>
          <prop k="offset_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="offset_unit" v="MM"/>
          <prop k="outline_color" v="255,255,255,255"/>
          <prop k="outline_style" v="solid"/>
          <prop k="outline_width" v="0.4"/>
          <prop k="outline_width_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="outline_width_unit" v="MM"/>
          <prop k="scale_method" v="area"/>
          <prop k="size" v="2.8"/>
          <prop k="size_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="size_unit" v="MM"/>
          <prop k="vertical_anchor_point" v="1"/>
          <effect type="effectStack" enabled="0">
            <effect type="drawSource">
              <Option type="Map">
                <Option type="QString" name="blend_mode" value="0"/>
                <Option type="QString" name="draw_mode" value="2"/>
                <Option type="QString" name="enabled" value="1"/>
                <Option type="QString" name="opacity" value="1"/>
              </Option>
              <prop k="blend_mode" v="0"/>
              <prop k="draw_mode" v="2"/>
              <prop k="enabled" v="1"/>
              <prop k="opacity" v="1"/>
            </effect>
          </effect>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
    </symbols>
  </renderer-v2>
  <customproperties>
    <Option type="Map">
      <Option type="QString" name="embeddedWidgets/count" value="0"/>
      <Option name="variableNames"/>
      <Option name="variableValues"/>
    </Option>
  </customproperties>
  <blendMode>0</blendMode>
  <featureBlendMode>0</featureBlendMode>
  <layerOpacity>1</layerOpacity>
  <SingleCategoryDiagramRenderer attributeLegend="1" diagramType="Histogram">
    <DiagramCategory spacingUnit="MM" backgroundColor="#ffffff" barWidth="5" maxScaleDenominator="1e+08" scaleDependency="Area" minScaleDenominator="500" rotationOffset="270" penAlpha="255" minimumSize="0" showAxis="0" penWidth="0" direction="1" sizeType="MM" height="15" penColor="#000000" enabled="0" lineSizeType="MM" scaleBasedVisibility="0" backgroundAlpha="255" spacing="0" diagramOrientation="Up" opacity="1" sizeScale="3x:0,0,0,0,0,0" labelPlacementMethod="XHeight" lineSizeScale="3x:0,0,0,0,0,0" width="15" spacingUnitScale="3x:0,0,0,0,0,0">
      <fontProperties style="" description="MS Shell Dlg 2,8.25,-1,5,50,0,0,0,0,0"/>
      <attribute field="" label="" color="#000000"/>
      <axisSymbol>
        <symbol alpha="1" type="line" name="" force_rhr="0" clip_to_extent="1">
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
          <layer locked="0" class="SimpleLine" enabled="1" pass="0">
            <Option type="Map">
              <Option type="QString" name="align_dash_pattern" value="0"/>
              <Option type="QString" name="capstyle" value="square"/>
              <Option type="QString" name="customdash" value="5;2"/>
              <Option type="QString" name="customdash_map_unit_scale" value="3x:0,0,0,0,0,0"/>
              <Option type="QString" name="customdash_unit" value="MM"/>
              <Option type="QString" name="dash_pattern_offset" value="0"/>
              <Option type="QString" name="dash_pattern_offset_map_unit_scale" value="3x:0,0,0,0,0,0"/>
              <Option type="QString" name="dash_pattern_offset_unit" value="MM"/>
              <Option type="QString" name="draw_inside_polygon" value="0"/>
              <Option type="QString" name="joinstyle" value="bevel"/>
              <Option type="QString" name="line_color" value="35,35,35,255"/>
              <Option type="QString" name="line_style" value="solid"/>
              <Option type="QString" name="line_width" value="0.26"/>
              <Option type="QString" name="line_width_unit" value="MM"/>
              <Option type="QString" name="offset" value="0"/>
              <Option type="QString" name="offset_map_unit_scale" value="3x:0,0,0,0,0,0"/>
              <Option type="QString" name="offset_unit" value="MM"/>
              <Option type="QString" name="ring_filter" value="0"/>
              <Option type="QString" name="trim_distance_end" value="0"/>
              <Option type="QString" name="trim_distance_end_map_unit_scale" value="3x:0,0,0,0,0,0"/>
              <Option type="QString" name="trim_distance_end_unit" value="MM"/>
              <Option type="QString" name="trim_distance_start" value="0"/>
              <Option type="QString" name="trim_distance_start_map_unit_scale" value="3x:0,0,0,0,0,0"/>
              <Option type="QString" name="trim_distance_start_unit" value="MM"/>
              <Option type="QString" name="tweak_dash_pattern_on_corners" value="0"/>
              <Option type="QString" name="use_custom_dash" value="0"/>
              <Option type="QString" name="width_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            </Option>
            <prop k="align_dash_pattern" v="0"/>
            <prop k="capstyle" v="square"/>
            <prop k="customdash" v="5;2"/>
            <prop k="customdash_map_unit_scale" v="3x:0,0,0,0,0,0"/>
            <prop k="customdash_unit" v="MM"/>
            <prop k="dash_pattern_offset" v="0"/>
            <prop k="dash_pattern_offset_map_unit_scale" v="3x:0,0,0,0,0,0"/>
            <prop k="dash_pattern_offset_unit" v="MM"/>
            <prop k="draw_inside_polygon" v="0"/>
            <prop k="joinstyle" v="bevel"/>
            <prop k="line_color" v="35,35,35,255"/>
            <prop k="line_style" v="solid"/>
            <prop k="line_width" v="0.26"/>
            <prop k="line_width_unit" v="MM"/>
            <prop k="offset" v="0"/>
            <prop k="offset_map_unit_scale" v="3x:0,0,0,0,0,0"/>
            <prop k="offset_unit" v="MM"/>
            <prop k="ring_filter" v="0"/>
            <prop k="trim_distance_end" v="0"/>
            <prop k="trim_distance_end_map_unit_scale" v="3x:0,0,0,0,0,0"/>
            <prop k="trim_distance_end_unit" v="MM"/>
            <prop k="trim_distance_start" v="0"/>
            <prop k="trim_distance_start_map_unit_scale" v="3x:0,0,0,0,0,0"/>
            <prop k="trim_distance_start_unit" v="MM"/>
            <prop k="tweak_dash_pattern_on_corners" v="0"/>
            <prop k="use_custom_dash" v="0"/>
            <prop k="width_map_unit_scale" v="3x:0,0,0,0,0,0"/>
            <data_defined_properties>
              <Option type="Map">
                <Option type="QString" name="name" value=""/>
                <Option name="properties"/>
                <Option type="QString" name="type" value="collection"/>
              </Option>
            </data_defined_properties>
          </layer>
        </symbol>
      </axisSymbol>
    </DiagramCategory>
  </SingleCategoryDiagramRenderer>
  <DiagramLayerSettings placement="0" zIndex="0" showAll="1" priority="0" dist="0" linePlacementFlags="18" obstacle="0">
    <properties>
      <Option type="Map">
        <Option type="QString" name="name" value=""/>
        <Option name="properties"/>
        <Option type="QString" name="type" value="collection"/>
      </Option>
    </properties>
  </DiagramLayerSettings>
  <geometryOptions geometryPrecision="0" removeDuplicateNodes="0">
    <activeChecks/>
    <checkConfiguration/>
  </geometryOptions>
  <legend type="default-vector" showLabelLegend="0"/>
  <referencedLayers/>
  <fieldConfiguration>
    <field name="OBJECTID" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="CODE" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="NAAM" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="DOORSTROOMBREEDTE" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="KRUINBREEDTE" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="LAAGSTEDOORSTROOMHOOGTE" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="HOOGSTEDOORSTROOMHOOGTE" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="SOORTREGELBAARHEID" configurationFlags="None">
      <editWidget type="Range">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="WS_CATEGORIE" configurationFlags="None">
      <editWidget type="Range">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="WS_KRUINVORM" configurationFlags="None">
      <editWidget type="Range">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="WS_FUNCTIESTUW" configurationFlags="None">
      <editWidget type="Range">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
  </fieldConfiguration>
  <aliases>
    <alias index="0" field="OBJECTID" name=""/>
    <alias index="1" field="CODE" name=""/>
    <alias index="2" field="NAAM" name=""/>
    <alias index="3" field="DOORSTROOMBREEDTE" name=""/>
    <alias index="4" field="KRUINBREEDTE" name=""/>
    <alias index="5" field="LAAGSTEDOORSTROOMHOOGTE" name=""/>
    <alias index="6" field="HOOGSTEDOORSTROOMHOOGTE" name=""/>
    <alias index="7" field="SOORTREGELBAARHEID" name=""/>
    <alias index="8" field="WS_CATEGORIE" name=""/>
    <alias index="9" field="WS_KRUINVORM" name=""/>
    <alias index="10" field="WS_FUNCTIESTUW" name=""/>
  </aliases>
  <defaults>
    <default field="OBJECTID" expression="" applyOnUpdate="0"/>
    <default field="CODE" expression="" applyOnUpdate="0"/>
    <default field="NAAM" expression="" applyOnUpdate="0"/>
    <default field="DOORSTROOMBREEDTE" expression="" applyOnUpdate="0"/>
    <default field="KRUINBREEDTE" expression="" applyOnUpdate="0"/>
    <default field="LAAGSTEDOORSTROOMHOOGTE" expression="" applyOnUpdate="0"/>
    <default field="HOOGSTEDOORSTROOMHOOGTE" expression="" applyOnUpdate="0"/>
    <default field="SOORTREGELBAARHEID" expression="" applyOnUpdate="0"/>
    <default field="WS_CATEGORIE" expression="" applyOnUpdate="0"/>
    <default field="WS_KRUINVORM" expression="" applyOnUpdate="0"/>
    <default field="WS_FUNCTIESTUW" expression="" applyOnUpdate="0"/>
  </defaults>
  <constraints>
    <constraint field="OBJECTID" notnull_strength="1" constraints="3" exp_strength="0" unique_strength="1"/>
    <constraint field="CODE" notnull_strength="0" constraints="0" exp_strength="0" unique_strength="0"/>
    <constraint field="NAAM" notnull_strength="0" constraints="0" exp_strength="0" unique_strength="0"/>
    <constraint field="DOORSTROOMBREEDTE" notnull_strength="0" constraints="0" exp_strength="0" unique_strength="0"/>
    <constraint field="KRUINBREEDTE" notnull_strength="0" constraints="0" exp_strength="0" unique_strength="0"/>
    <constraint field="LAAGSTEDOORSTROOMHOOGTE" notnull_strength="0" constraints="0" exp_strength="0" unique_strength="0"/>
    <constraint field="HOOGSTEDOORSTROOMHOOGTE" notnull_strength="0" constraints="0" exp_strength="0" unique_strength="0"/>
    <constraint field="SOORTREGELBAARHEID" notnull_strength="0" constraints="0" exp_strength="0" unique_strength="0"/>
    <constraint field="WS_CATEGORIE" notnull_strength="0" constraints="0" exp_strength="0" unique_strength="0"/>
    <constraint field="WS_KRUINVORM" notnull_strength="0" constraints="0" exp_strength="0" unique_strength="0"/>
    <constraint field="WS_FUNCTIESTUW" notnull_strength="0" constraints="0" exp_strength="0" unique_strength="0"/>
  </constraints>
  <constraintExpressions>
    <constraint field="OBJECTID" desc="" exp=""/>
    <constraint field="CODE" desc="" exp=""/>
    <constraint field="NAAM" desc="" exp=""/>
    <constraint field="DOORSTROOMBREEDTE" desc="" exp=""/>
    <constraint field="KRUINBREEDTE" desc="" exp=""/>
    <constraint field="LAAGSTEDOORSTROOMHOOGTE" desc="" exp=""/>
    <constraint field="HOOGSTEDOORSTROOMHOOGTE" desc="" exp=""/>
    <constraint field="SOORTREGELBAARHEID" desc="" exp=""/>
    <constraint field="WS_CATEGORIE" desc="" exp=""/>
    <constraint field="WS_KRUINVORM" desc="" exp=""/>
    <constraint field="WS_FUNCTIESTUW" desc="" exp=""/>
  </constraintExpressions>
  <expressionfields/>
  <attributeactions>
    <defaultAction value="{00000000-0000-0000-0000-000000000000}" key="Canvas"/>
  </attributeactions>
  <attributetableconfig sortExpression="" sortOrder="0" actionWidgetStyle="dropDown">
    <columns>
      <column hidden="0" width="-1" type="field" name="OBJECTID"/>
      <column hidden="0" width="-1" type="field" name="CODE"/>
      <column hidden="0" width="-1" type="field" name="NAAM"/>
      <column hidden="0" width="-1" type="field" name="DOORSTROOMBREEDTE"/>
      <column hidden="0" width="-1" type="field" name="KRUINBREEDTE"/>
      <column hidden="0" width="-1" type="field" name="LAAGSTEDOORSTROOMHOOGTE"/>
      <column hidden="0" width="-1" type="field" name="HOOGSTEDOORSTROOMHOOGTE"/>
      <column hidden="0" width="-1" type="field" name="SOORTREGELBAARHEID"/>
      <column hidden="0" width="-1" type="field" name="WS_CATEGORIE"/>
      <column hidden="0" width="-1" type="field" name="WS_KRUINVORM"/>
      <column hidden="0" width="-1" type="field" name="WS_FUNCTIESTUW"/>
      <column hidden="1" width="-1" type="actions"/>
    </columns>
  </attributetableconfig>
  <conditionalstyles>
    <rowstyles/>
    <fieldstyles/>
  </conditionalstyles>
  <storedexpressions/>
  <editform tolerant="1"></editform>
  <editforminit/>
  <editforminitcodesource>0</editforminitcodesource>
  <editforminitfilepath></editforminitfilepath>
  <editforminitcode><![CDATA[# -*- coding: utf-8 -*-
"""
Formulieren van QGIS kunnen een functie voor Python hebben die wordt aangeroepen wanneer het formulier wordt geopend.

Gebruik deze functie om extra logica aan uw formulieren toe te voegen.

Voer de naam van de functie in in het veld "Python Init function".

Een voorbeeld volgt hieronder:
"""
from qgis.PyQt.QtWidgets import QWidget

def my_form_open(dialog, layer, feature):
	geom = feature.geometry()
	control = dialog.findChild(QWidget, "MyLineEdit")
]]></editforminitcode>
  <featformsuppress>0</featformsuppress>
  <editorlayout>generatedlayout</editorlayout>
  <editable>
    <field name="CODE" editable="1"/>
    <field name="DAMO_W.Stuw.CODE" editable="1"/>
    <field name="DAMO_W.Stuw.DOORSTROOMBREEDTE" editable="1"/>
    <field name="DAMO_W.Stuw.GISOBJID" editable="1"/>
    <field name="DAMO_W.Stuw.HOOGSTEDOORSTROOMHOOGTE" editable="1"/>
    <field name="DAMO_W.Stuw.HYPERLINK" editable="1"/>
    <field name="DAMO_W.Stuw.INDICATIEWATERKEREND" editable="1"/>
    <field name="DAMO_W.Stuw.LAAGSTEDOORSTROOMHOOGTE" editable="1"/>
    <field name="DAMO_W.Stuw.NAAM" editable="1"/>
    <field name="DAMO_W.Stuw.OBJECTID" editable="1"/>
    <field name="DAMO_W.Stuw.OPMERKING" editable="1"/>
    <field name="DAMO_W.Stuw.SOORTREGELBAARHEID" editable="1"/>
    <field name="DAMO_W.Stuw.SOORTSTUW" editable="1"/>
    <field name="DAMO_W.Stuw.WS_BRON" editable="1"/>
    <field name="DAMO_W.Stuw.WS_CATEGORIE" editable="1"/>
    <field name="DAMO_W.Stuw.WS_FUNCTIESTUW" editable="1"/>
    <field name="DAMO_W.Stuw.WS_INFORCODE" editable="1"/>
    <field name="DAMO_W.Stuw.WS_INWINNINGSDATUM" editable="1"/>
    <field name="DAMO_W.Stuw.WS_INWINNINGSDATUM_ADMIN" editable="1"/>
    <field name="DAMO_W.Stuw.WS_INWINNINGSWIJZE" editable="1"/>
    <field name="DAMO_W.Stuw.WS_INWINNINGSWIJZE_ADMIN" editable="1"/>
    <field name="DAMO_W.Stuw.WS_KRUINVORM" editable="1"/>
    <field name="DAMO_W.Stuw.WS_KWALITEITSSCORE" editable="1"/>
    <field name="DAMO_W.Stuw.WS_OHPLICHT_KWK" editable="1"/>
    <field name="DAMO_W.Stuw.WS_OP_AFSTAND_BEHEERD" editable="1"/>
    <field name="DAMO_W.Stuw.WS_VOLLEDIGHEID_DATASET" editable="1"/>
    <field name="DAMO_W.WS_OHPLICHT_KWK.WS_OHPLICHT_AFVAL" editable="1"/>
    <field name="DAMO_W.WS_OHPLICHT_KWK.WS_OHPLICHT_CONSTRUCTIE" editable="1"/>
    <field name="DAMO_W.WS_OHPLICHT_KWK.WS_OHPLICHT_DROOG" editable="1"/>
    <field name="DAMO_W.WS_OHPLICHT_KWK.WS_OHPLICHT_NAT" editable="1"/>
    <field name="DOORSTROOMBREEDTE" editable="1"/>
    <field name="HOOGSTEDOORSTROOMHOOGTE" editable="1"/>
    <field name="KRUINBREEDTE" editable="1"/>
    <field name="LAAGSTEDOORSTROOMHOOGTE" editable="1"/>
    <field name="NAAM" editable="1"/>
    <field name="OBJECTID" editable="1"/>
    <field name="SOORTREGELBAARHEID" editable="1"/>
    <field name="WS_CATEGORIE" editable="1"/>
    <field name="WS_FUNCTIESTUW" editable="1"/>
    <field name="WS_KRUINVORM" editable="1"/>
    <field name="kunstwerkxy.coordinaten" editable="1"/>
  </editable>
  <labelOnTop>
    <field name="CODE" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.CODE" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.DOORSTROOMBREEDTE" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.GISOBJID" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.HOOGSTEDOORSTROOMHOOGTE" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.HYPERLINK" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.INDICATIEWATERKEREND" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.LAAGSTEDOORSTROOMHOOGTE" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.NAAM" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.OBJECTID" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.OPMERKING" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.SOORTREGELBAARHEID" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.SOORTSTUW" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.WS_BRON" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.WS_CATEGORIE" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.WS_FUNCTIESTUW" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.WS_INFORCODE" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.WS_INWINNINGSDATUM" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.WS_INWINNINGSDATUM_ADMIN" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.WS_INWINNINGSWIJZE" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.WS_INWINNINGSWIJZE_ADMIN" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.WS_KRUINVORM" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.WS_KWALITEITSSCORE" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.WS_OHPLICHT_KWK" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.WS_OP_AFSTAND_BEHEERD" labelOnTop="0"/>
    <field name="DAMO_W.Stuw.WS_VOLLEDIGHEID_DATASET" labelOnTop="0"/>
    <field name="DAMO_W.WS_OHPLICHT_KWK.WS_OHPLICHT_AFVAL" labelOnTop="0"/>
    <field name="DAMO_W.WS_OHPLICHT_KWK.WS_OHPLICHT_CONSTRUCTIE" labelOnTop="0"/>
    <field name="DAMO_W.WS_OHPLICHT_KWK.WS_OHPLICHT_DROOG" labelOnTop="0"/>
    <field name="DAMO_W.WS_OHPLICHT_KWK.WS_OHPLICHT_NAT" labelOnTop="0"/>
    <field name="DOORSTROOMBREEDTE" labelOnTop="0"/>
    <field name="HOOGSTEDOORSTROOMHOOGTE" labelOnTop="0"/>
    <field name="KRUINBREEDTE" labelOnTop="0"/>
    <field name="LAAGSTEDOORSTROOMHOOGTE" labelOnTop="0"/>
    <field name="NAAM" labelOnTop="0"/>
    <field name="OBJECTID" labelOnTop="0"/>
    <field name="SOORTREGELBAARHEID" labelOnTop="0"/>
    <field name="WS_CATEGORIE" labelOnTop="0"/>
    <field name="WS_FUNCTIESTUW" labelOnTop="0"/>
    <field name="WS_KRUINVORM" labelOnTop="0"/>
    <field name="kunstwerkxy.coordinaten" labelOnTop="0"/>
  </labelOnTop>
  <reuseLastValue>
    <field name="CODE" reuseLastValue="0"/>
    <field name="DOORSTROOMBREEDTE" reuseLastValue="0"/>
    <field name="HOOGSTEDOORSTROOMHOOGTE" reuseLastValue="0"/>
    <field name="KRUINBREEDTE" reuseLastValue="0"/>
    <field name="LAAGSTEDOORSTROOMHOOGTE" reuseLastValue="0"/>
    <field name="NAAM" reuseLastValue="0"/>
    <field name="OBJECTID" reuseLastValue="0"/>
    <field name="SOORTREGELBAARHEID" reuseLastValue="0"/>
    <field name="WS_CATEGORIE" reuseLastValue="0"/>
    <field name="WS_FUNCTIESTUW" reuseLastValue="0"/>
    <field name="WS_KRUINVORM" reuseLastValue="0"/>
  </reuseLastValue>
  <dataDefinedFieldProperties/>
  <widgets/>
  <previewExpression>"CODE"</previewExpression>
  <mapTip></mapTip>
  <layerGeometryType>0</layerGeometryType>
</qgis>
