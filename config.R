#QUERIES
# Select queries to run: 
# SELECTED.QUERIES <- c(7, 9): runs query number 7 and 9 
# SELECTED.QUERIES <- c(2:5): runs queries from 2 to 5
# SELECTED.QUERIES <- c() or NULL : runs all queries in queries_xml list (default)
SELECTED.QUERIES <- NULL

#DATABASES
# Select dbs to query: list the names of the dbs name
# SELECTED.DBs <- c("database_basexdb_5p4_nze02", ..): runs query on db name: "database_basexdb_5p4_nze02" 
# SELECTED.DBs <- c() or NULL for all dbs in the folder (default)
SELECTED.DBs <- c() 

#RUN TYPE = {'seperate', 'aggregate'}
# seperate: each query for reach db is placed in a file
# aggregate: result of running a query on all dbs is placed in a file
RUN.TYPE <- "seperate" 

#MAIN_QUERY 
# file where original queries are placed in gcam/output folder. Uses forward-slash path seperator i.e. "/"
# MAIN.QUERY <- "/path/to/Main_queries.xml"
# Note: a path to main query file must be provided
MAIN.QUERY <- NULL

#QUERY.BY
# QUERY.BY takes either "title" or "xml"
# title: the query's title (in queries_xml list) will be used to extract the query's xml from MAIN.QUERY file. (default)
# xml: the actual xml query (provided in queries_xml list) will be used to to run the query 
QUERY.BY <- "title"

#REGIONS 
# Specify the region(s) to run on. 
# REGIONS <- c('USA', 'Canada') for any set of countries such as USA and Canada
# REGIONS <- c() or NULL means Global region (default)
REGIONS <- c('Global')


#QUERIES_XMLs 
# SYNTAX: list( number = list (query.title = query.xml), ...)
#  Example of a new query to add to the list queries_xml: 
#    number: 19, query.title: population by region, query.xmm: <demographicsQuery>.. </demographicsQuery>
#       "19" = list(
#       "population by region" = '<demographicsQuery title="population by region">
#                                       <axis1 name="region">region</axis1>
#                                       <axis2 name="Year">populationMiniCAM</axis2>
#                                       <xPath buildList="true" dataName="total-population" group="false" sumAll="false">
#                                       demographics/populationMiniCAM/total-population/node()
#                                       </xPath>
#                                   <comments/>
#                                   </demographicsQuery>'

queries_xml <- list(
  #QUERY_1 -- 
  "1" = list(
    "population by region" = '<demographicsQuery title="population by region">
  <axis1 name="region">region</axis1>
  <axis2 name="Year">populationMiniCAM</axis2>
  <xPath buildList="true" dataName="total-population" group="false" sumAll="false">demographics/populationMiniCAM/total-population/node()</xPath>
  <comments/>
  </demographicsQuery>'),
  
  #QUERY_2 -- 
  "2" = list(
    "CO2 concentrations" = '<ClimateQuery title="CO2 concentrations">
  <axis1 name="CO2-concentration">none</axis1>
  <axis2 name="Year">CO2-concentration[@year]</axis2>
  <xPath buildList="true" dataName="CO2-concentration" group="false" sumAll="false">climate-model/CO2-concentration/text()</xPath>
  <comments/>
  </ClimateQuery>'),
  
  #QUERY_3 -- 
  "3" = list(
    "total climate forcing" = '<ClimateQuery title="total climate forcing">
  <axis1 name="forcing-total">none</axis1>
  <axis2 name="Year">forcing-total[@year]</axis2>
  <xPath buildList="true" dataName="forcing-total" group="false" sumAll="false">climate-model/forcing-total/text()</xPath>
  <comments/>
  </ClimateQuery>'),
  
  #QUERY_4 -- 
  "4" = list(
    "global mean temperature" = '<ClimateQuery title="global mean temperature">
    <axis1 name="temperature">none</axis1>
    <axis2 name="Year">global-mean-temperature[@year]</axis2>
    <xPath buildList="true" dataName="global-mean-temperature"
        group="false" sumAll="false">climate-model/global-mean-temperature/text()</xPath>
    <comments/>
</ClimateQuery>'),
  
  #QUERY_5 -- 
  "5" = list(
    "CO2 emissions by region" = '<emissionsQueryBuilder title="CO2 emissions by region">
    <axis1 name="region">region</axis1>
    <axis2 name="Year">emissions</axis2>
    <xPath buildList="true" dataName="emissions" group="false" sumAll="false">*[@type = \'sector\' (:collapse:) or @type = \'resource\' (:collapse:)](: / *[@type = \'subresource\' (: collapse :)] :)//*[((@name=\'CO2\'))]/emissions/node()</xPath>
    <comments/>
</emissionsQueryBuilder>'),
  
  #QUERY_6 -- 
  "6" = list(
    "nonCO2 emissions by subsector" = '<emissionsQueryBuilder title="nonCO2 emissions by subsector">
    <axis1 name="GHG">GHG</axis1>
    <axis2 name="Year">emissions</axis2>
    <xPath buildList="true" dataName="emissions" group="false" sumAll="false">*[@type = \'sector\']/*[@type = \'subsector\']//
            *[@type = \'GHG\']/emissions/node()</xPath>
    <comments/>
</emissionsQueryBuilder>'),
  
  #QUERY_7 -- 
  "7" = list(
    "CO2 emissions by assigned sector (no bio)" = '<supplyDemandQuery title="CO2 emissions by assigned sector (no bio)">
    <axis1 name="sector">sector[@name]</axis1>
    <axis2 name="Year">emissions[@year]</axis2>
    <xPath buildList="true" dataName="input" group="false" sumAll="false"><![CDATA[

               declare function local:append-heirarchy($parent as node(), $append as node()*) as node() {
	       	 		 let $scn := $parent/ancestor::scenario,
	       			   	  $rgn := $parent (: /ancestor::region :)
	       			   return
	       			   	  document { element scenario {
	       			 	  					$scn/@*,
	       			 						element region {
	       			 							$rgn/@*,
	       			 							$append
	       			 						}
	       			 	  				}
	       				}
	       	 		 (: I can get by with just the scenario and region
	       			 let $new_node := element {local-name($parent)} {$parent/@*, $append}
	       	 		 return
	       	 		 if(local-name($parent) != \'scenario\')
	       	 		 then local:append-heirarchy($parent/parent::*, $new_node)
	       	 		 else document { $new_node } :)
	       	 	 };
	       	 	 declare function local:generate-sector-output-coefs($inputNameQueue as xs:string*, $currTree as node(), $coefs as node()*, $is_usa as xs:boolean) as node()* {
                 if(empty($inputNameQueue)) then $coefs
                 else if( exists($coefs[@name = $inputNameQueue[1]]) or exists(index-of((\'unconventional oil production\', "electricity", "cement", "N fertilizer"),
$inputNameQueue[1])) or not($currTree/*[@type=\'sector\' and @name=$inputNameQueue[1]]))
then
local:generate-sector-output-coefs(remove($inputNameQueue, 1), $currTree, $coefs, $is_usa)
	       				else
                    let $inputName := $inputNameQueue[1],
                        $newInputNameQueue := remove($inputNameQueue, 1),
                        $useInputs := $currTree//*[@type=\'input\' and @name=$inputName],
                        $useSectors := distinct-values($useInputs/ancestor::*[@type=\'sector\']/@name),
                        $totalInputSum := for $vintage in distinct-values($useInputs/demand-physical/@vintage)
                                          return element input {
                                                     attribute vintage { $vintage },
                                                     text {
                                                         sum($useInputs/demand-physical[@vintage=$vintage])
                                                     }
                                                 },
                       $new_coefs := if(empty($useSectors)) then
                                         $coefs
                                     else
                                         $coefs | element sector {
                                            attribute name { $inputName },
                                            for $output in $useSectors
                                            return element output {
                                                       attribute name { $output },
                                                       for $inputSum in $totalInputSum
                                                       let $outputSum := sum($useInputs[ancestor::*[@type=\'sector\' and @name=$output]]/demand-physical[@vintage=$inputSum/@vintage])
                                                       return element coef {
                                                                  attribute vintage { $inputSum/@vintage },
                                                                  text { $outputSum div $inputSum }
                                                              }
                                                    }
                                        }
                        return
                              local:generate-sector-output-coefs(distinct-values(($newInputNameQueue, $useSectors)), $currTree, $new_coefs, $is_usa)
		};
        declare function local:apply-coefs($outputName as xs:string, $emissions as node()*, $coefs as node()*) as node()* {
            if(exists($coefs[@name=$outputName]) and abs(sum($emissions)) > 0.001) then
                for $output in $coefs[@name=$outputName]/output
                return local:apply-coefs($output/@name,
                    for $year in distinct-values($emissions/@year)
                    let $emissThisVintage := $emissions[@year=$year],
                        $firstEmiss := $emissThisVintage[1],
                        $emissSum := sum($emissThisVintage),
                        $coefThisVintage := $output/coef[@vintage=$year]
                    where $coefThisVintage > 0
                    return element { local-name($firstEmiss) } {
                            $firstEmiss/@*,
                            text{ $emissSum * $coefThisVintage }
                        }
	       			, $coefs)
            else if( abs(sum($emissions)) > 0.001) then
                element sector {
                    attribute name { $outputName },
                    attribute type { \'sector\' },
                    (: $emissions :) (: TODO: not sure why this doesn\'t work and we need to create these explicitly :)
for $e in $emissions
return element emissions { $e/@*, text{ $e/text() } }
}
else
  (: These are the residuals from chasing simulenaties, I\'ve left this here
                   for debuging purposes :)
                element sector {
                    attribute name { $outputName },
                    attribute type { \'sector\' }(:,
                    $emissions:)
                }
        };
		declare function local:run-emiss-by-enduse($scenarios as xs:string*, $regions as xs:string*, $collection as xs:string) as node()* {
			 	 unordered {
			 	 let $regionsG := if(not($regions[1] = \'Global\'))
			 	 		  then $regions
			 	 		  else distinct-values(collection($collection)/scenario/world/*[@type=\'region\']/@name)
			 	 return
			 	 for $scenario in $scenarios,
			 	 $region in $regionsG
			 	 let $scenario_split := tokenize($scenario, \' \'),
				 $currTree := collection($collection)/scenario[@name = $scenario_split[1] and @date = $scenario_split[2]]/world/*[@type=\'region\' and @name=$region],
                 $currEmissSectors := $currTree/*[@type=\'sector\' and descendant::CO2],
                 $coefs := local:generate-sector-output-coefs(distinct-values($currEmissSectors/@name), $currTree, (), false())
				 return
				    for $sectorName in distinct-values($currEmissSectors/@name)
                    return local:append-heirarchy($currTree, local:apply-coefs($sectorName, $currEmissSectors[@name=$sectorName]//CO2/emissions, $coefs))//text()
			 	 }
	 	 };
		 local:run-emiss-by-enduse((:scenarios:), (:regions:), (:collection:))


                ]]></xPath>
    <comments/>
    <labelRewriteList append-values="false">
        <level name="sector">
            <rewrite from="trn_pass_road_LDV_4W" to="transportation"/>
            <rewrite from="trn_pass_road" to="transportation"/>
            <rewrite from="trn_pass_road_LDV_2W" to="transportation"/>
            <rewrite from="trn_freight_road" to="transportation"/>
            <rewrite from="trn_passenger" to="transportation"/>
            <rewrite from="trn_freight" to="transportation"/>
            <rewrite from="comm others" to="buildings"/>
            <rewrite from="comm heating" to="buildings"/>
            <rewrite from="comm cooling" to="buildings"/>
            <rewrite from="trn_pass_road_LDV" to="transportation"/>
            <rewrite from="trn_pass_road_bus" to="transportation"/>
            <rewrite from="trn_aviation_intl" to="transportation"/>
            <rewrite from="trn_pass" to="transportation"/>
            <rewrite from="N fertilizer" to="industry"/>
            <rewrite from="resid heating" to="buildings"/>
            <rewrite from="resid others" to="buildings"/>
            <rewrite from="unconventional oil production" to="industry"/>
            <rewrite from="resid cooling" to="buildings"/>
            <rewrite from="trn_shipping_intl" to="transportation"/>
        </level>
    </labelRewriteList>
</supplyDemandQuery>'),
  
  #QUERY_8  -- 
  "8" = list(
    "CO2 emissions by tech" = '<emissionsQueryBuilder title="CO2 emissions by tech">
    <axis1 name="technology">technology</axis1>
    <axis2 name="Year">emissions</axis2>
    <xPath buildList="true" dataName="emissions" group="false" sumAll="false">*[@type = \'sector\' ]/*[@type=\'subsector\']/*[@type=\'technology\']//
            CO2/emissions/node()</xPath>
    <comments/>
</emissionsQueryBuilder>'),
  
  #QUERY_9 -- 
  "9" = list(
    "CO2 prices (global)" = '<marketQuery title="CO2 prices (global)">
    <axis1 name="market">market</axis1>
    <axis2 name="Year">market</axis2>
    <xPath buildList="true" dataName="price" group="false" sumAll="false">Marketplace/market[true() and contains(@name,\'globalCO2\')]/price/node()</xPath>
    <comments/>
</marketQuery>'),
  
  #QUERY_10 -- 
  "10" = list(
    "policy cost by period" = '<costCurveQuery title="policy cost by period">
    <axis1 name="region">Curve</axis1>
    <axis2 name="Year">DataPoint</axis2>
    <xPath buildList="true" dataName="Cost" group="false" sumAll="false">PointSet/DataPoint/y/text()</xPath>
    <comments/>
</costCurveQuery>'),
  
  #QUERY_11 -- 
  "11" = list(
    "undiscounted policy cost" = '<costCurveQuery title="undiscounted policy cost">
    <axis1 name="UndiscountedCost">UndiscountedCost</axis1>
    <axis2 name="region">UndiscountedCost</axis2>
    <xPath buildList="true" dataName="Undiscounted Cost" group="false" sumAll="false">/text()</xPath>
    <comments/>
</costCurveQuery>'),
  
  #QUERY_12 -- 
  "12" = list(
    "discounted policy cost" = '<costCurveQuery title="discounted policy cost">
    <axis1 name="DiscountedCost">DiscountedCost</axis1>
    <axis2 name="region">DiscountedCost</axis2>
    <xPath buildList="true" dataName="Discounted Cost" group="false" sumAll="false">/text()</xPath>
    <comments/>
</costCurveQuery>'),
  
  #QUERY_13 -- 
  "13" = list(
    "primary energy consumption with CCS by region (direct equivalent)" = '<supplyDemandQuery title="primary energy consumption with CCS by region (direct equivalent)">
    <axis1 name="fuel">input[@name]</axis1>
    <axis2 name="Year">demand-physical[@vintage]</axis2>
    <xPath buildList="true" dataName="input" group="false" sumAll="false"><![CDATA[

               declare function local:append-heirarchy($parent as node(), $append as node()*) as node() {
	       	 		 let $scn := $parent/ancestor::scenario,
	       			   	  $rgn := $parent (: /ancestor::region :)
	       			   return
	       			   	  document { element scenario {
	       			 	  					$scn/@*,
	       			 						element region {
	       			 							$rgn/@*,
	       			 							$append
	       			 						}
	       			 	  				}
	       				}
	       	 		 (: I can get by with just the scenario and region
	       			 let $new_node := element {local-name($parent)} {$parent/@*, $append}
	       	 		 return
	       	 		 if(local-name($parent) != \'scenario\')
	       	 		 then local:append-heirarchy($parent/parent::*, $new_node)
	       	 		 else document { $new_node } :)
	       	 	 };
	       	 	 declare function local:generate-sector-input-coefs($outputNameQueue as xs:string*, $currTree as node(), $coefs as node()*, $is_usa as xs:boolean) as node()* {
                 if(empty($outputNameQueue)) then $coefs
                 else if( exists($coefs[@name = $outputNameQueue[1]]) or exists(index-of((\'biomass\',
\'traded oil\', \'traded coal\', \'traded natural gas\', \'regional corn for ethanol\', \'regional biomassOil\', \'regional sugar for ethanol\', \'regional sugarbeet for ethanol\'),
$outputNameQueue[1])) or not($currTree/*[@type=\'sector\' and @name=$outputNameQueue[1]]))
then
(:if(not($is_usa) and string-length($currTree/@name) = 2) then
local:trace-inputs($outputName, $currTree/parent::*/*[@type=\'region\' and @name=\'USA\'], $outputs, true())
else:)
local:generate-sector-input-coefs(remove($outputNameQueue, 1), $currTree, $coefs, $is_usa)
	       				else
                    let $outputName := $outputNameQueue[1],
                        $newOutputNameQueue := remove($outputNameQueue, 1),
                        $useOutputs := $currTree//output-primary[@type=\'output\' and @name=$outputName],
                        $useInputs := for $out in $useOutputs[not(following-sibling::keyword[exists(@primary-renewable)])]
                                      return $out/following-sibling::*[@type=\'input\' and not(@name=\'oil-credits\') and not(starts-with(@name, \'water_td\'))],
                        $renewOutputs := for $out in $useOutputs[following-sibling::keyword[exists(@primary-renewable)]]
                                         return element output {
                                             attribute name { $out/following-sibling::keyword/@primary-renewable },
                                             $out/child::*
                                         },
                        $totalOutputSum := for $vintage in distinct-values($useOutputs/physical-output/@vintage)
                                          return element output {
                                                     attribute vintage { $vintage },
                                                     text {
                                                         sum($useOutputs/physical-output[@vintage=$vintage])
                                                     }
                                                 },
                       $new_coefs := $coefs | element sector {
                                            attribute name { $outputName },
                                            for $input in distinct-values($useInputs/@name)
                                            return element input {
                                                       attribute name { $input },
                                                       for $outputSum in $totalOutputSum
                                                       let $inputSum := sum($useInputs[@name=$input]/demand-physical[@vintage=$outputSum/@vintage])
                                                       where $inputSum > 0
                                                       return element coef {
                                                                  attribute vintage { $outputSum/@vintage },
                                                                  text { $inputSum div $outputSum }
                                                              }
                                                    },
                                            for $input in distinct-values($renewOutputs/@name)
                                            return element input {
                                                       attribute name { concat($input, \' renewable\') },
                                                       attribute is-renewable { true() },
                                                       for $outputSum in $totalOutputSum
                                                       let $inputSum := sum($renewOutputs[@name=$input]/physical-output[@vintage=$outputSum/@vintage])
                                                       where $inputSum > 0
                                                       return element coef {
                                                                  attribute vintage { $outputSum/@vintage },
                                                                  text { $inputSum div $outputSum }
                                                              }
                                                    }
                                        }
                        return
                              local:generate-sector-input-coefs(distinct-values(($newOutputNameQueue, $useInputs/@name)), $currTree, $new_coefs, $is_usa)
		};
        declare function local:generate-ccs-coefs($currTree as node(), $coefs as node()*) as node()* {
            for $sector in $coefs/@name
            let $currSector := $currTree/*[@type=\'sector\' and @name=$sector],
                $useInputs := $currSector//*[@type=\'technology\' and not(contains(@name, \'CCS\')) and not(child::keyword/@primary-renewable)]/*[@type=\'input\' and not(@name=\'oil-credits\') and not(starts-with(@name, \'water_td\'))],
                $useInputsCCS := $currSector//*[@type=\'technology\' and contains(@name, \'CCS\')]/*[@type=\'input\' and not(@name=\'oil-credits\') and not(starts-with(@name, \'water_td\'))],
                $totalOutputSum := for $vintage in distinct-values($useInputs/demand-physical/@vintage | $useInputsCCS/demand-physical/@vintage)
                                          return element output {
                                                     attribute vintage { $vintage },
                                                     text {
                                                         sum($currSector//output-primary/physical-output[@vintage=$vintage])
                                                     }
                                                 }
            return if(exists($useInputsCCS)) then
                element sector {
                                            attribute name { $sector },
                                            $coefs[@name=$sector]/input[@is-renewable],
                                            for $input in distinct-values(($useInputs/@name, $useInputsCCS/@name))
                                            return element input {
                                                       attribute name { $input },
                                                       for $outputSum in $totalOutputSum
                                                       let $inputSum := sum($useInputs[@name=$input]/demand-physical[@vintage=$outputSum/@vintage]),
                                                           $inputSumCCS := sum($useInputsCCS[@name=$input]/demand-physical[@vintage=$outputSum/@vintage])
                                                       return (element coef {
                                                                  attribute vintage { $outputSum/@vintage },
                                                                  text { $inputSum div $outputSum }
                                                              },
                                                              element coef_ccs {
                                                                  attribute vintage { $outputSum/@vintage },
                                                                  text { $inputSumCCS div $outputSum }
                                                              })
                                                    }
                                        }
                    else
                        $coefs[@name=$sector]
        };
        declare function local:apply-coefs($outputName as xs:string, $outputs as node()*, $coefs as node()*, $isCCS as xs:boolean) as node()* {
            if(exists($coefs[@name=$outputName]) and sum($outputs) > 0.001) then
                for $input in $coefs[@name=$outputName]/input
                return local:apply-coefs($input/@name,
                    for $vintage in distinct-values($outputs/@vintage)
                    let $outputThisVintage := $outputs[@vintage=$vintage],
                        $firstOutput := $outputThisVintage[1],
                        $outputSum := sum($outputThisVintage),
                        $coefThisVintage := $input/coef[@vintage=$vintage]
                    where $coefThisVintage > 0
	       		    return element { local-name($firstOutput) } {
	       				$firstOutput/@*,
                        text{ $outputSum * $coefThisVintage }
                        }, $coefs, $isCCS)
                    | local:apply-coefs($input/@name,
                    for $vintage in distinct-values($outputs/@vintage)
                    let $outputThisVintage := $outputs[@vintage=$vintage],
                        $firstOutput := $outputThisVintage[1],
                        $outputSum := sum($outputThisVintage),
                        $coefThisVintage := $input/coef_ccs[@vintage=$vintage]
                    where exists($coefThisVintage) and $coefThisVintage > 0
	       		    return element { local-name($firstOutput) } {
	       				$firstOutput/@*,
                        text{ $outputSum * $coefThisVintage }
	       			}, $coefs, true())
            else if( sum($outputs) > 0.001) then
                element input {
                    attribute name { if($isCCS) then concat($outputName, \' CCS\') else $outputName },
                    attribute type { \'input\' },
                    (: $outputs :) (: TODO: not sure why this doesn\'t work and we need to create these explicitly :)
for $o in $outputs
return element demand-physical { $o/@*, text{ $o/text() } }
}
else
  (: These are the residuals from chasing simulenaties, I\'ve left this here
                   for debuging purposes :)
                element input {
                    attribute name { $outputName },
                    attribute type { \'input\' } (:,
                    $outputs :)
                }
        };
		declare function local:run-input-by-primary($scenarios as xs:string*, $regions as xs:string*, $collection as xs:string) as node()* {
			 	 unordered {
			 	 let $regionsG := if(not($regions[1] = \'Global\'))
			 	 		  then $regions
			 	 		  else distinct-values(collection($collection)/scenario/world/*[@type=\'region\']/@name)
			 	 return
			 	 for $scenario in $scenarios,
			 	 $region in $regionsG
			 	 let $scenario_split := tokenize($scenario, \' \'),
				 $currTree := collection($collection)/scenario[@name = $scenario_split[1] and @date = $scenario_split[2]]/world/*[@type=\'region\' and @name=$region],
                 $currInputs := $currTree/*[@type=\'sector\' and (@name=\'unconventional oil production\' or exists(child::keyword/@final-energy))]//*[@type=\'input\' and empty(index-of((\'trn_pass_road\', \'limestone\', \'process heat cement\', \'industrial energy use\', \'industrial feedstocks\', \'renewable\', \'trn_freight_road\', \'trn_pass_road_LDV\', \'trn_pass_road_LDV_2W\', \'trn_pass_road_LDV_4W\', \'unconventional oil\', \'oil-credits\'), @name))],
                 $coefs := local:generate-sector-input-coefs(distinct-values($currInputs/@name), $currTree, (), false()),
                 $ccs_coefs := local:generate-ccs-coefs($currTree, $coefs)
				 return
				    for $inputName in distinct-values($currInputs/@name)
				    return local:append-heirarchy($currTree, local:apply-coefs($inputName, $currInputs[@name=$inputName]/demand-physical, $ccs_coefs, false()))//text()
			 	 }
	 	 };
		 local:run-input-by-primary((:scenarios:), (:regions:), (:collection:))


                ]]></xPath>
    <comments>WARNING: Results are slightly inflated due to improper secondary-output accounting</comments>
    <labelRewriteList append-values="false">
        <level name="input">
            <rewrite from="water consumption CCS" to=""/>
            <rewrite from="traditional biomass" to="j traditional biomass"/>
            <rewrite from="seawater CCS" to=""/>
            <rewrite from="coal CCS" to="c coal CCS"/>
            <rewrite from="wind-H2 renewable" to="g wind"/>
            <rewrite from="regional biomassOil CCS" to="d biomass CCS"/>
            <rewrite from="traded coal" to="c coal"/>
            <rewrite from="limestone" to=""/>
            <rewrite from="seawater" to=""/>
            <rewrite from="traded biomass CCS" to="d biomass CCS"/>
            <rewrite from="geothermal-elect renewable CCS" to="i geothermal"/>
            <rewrite from="hydro-elect renewable" to="f hydro"/>
            <rewrite from="wind-elect renewable" to="g wind"/>
            <rewrite from="nuclear-H2 renewable" to="e nuclear"/>
            <rewrite from="crude oil" to="a oil"/>
            <rewrite from="regional natural gas" to=""/>
            <rewrite from="traded coal CCS" to="c coal CCS"/>
            <rewrite from="natural gas CCS" to="b natural gas CCS"/>
            <rewrite from="traded biomass" to="d biomass"/>
            <rewrite from="geothermal-elect" to="i geothermal"/>
            <rewrite from="water consumption" to=""/>
            <rewrite from="renewable" to=""/>
            <rewrite from="water withdrawals CCS" to=""/>
            <rewrite from="coal" to="c coal"/>
            <rewrite from="traded natural gas CCS" to="b natural gas CCS"/>
            <rewrite from="regional corn for ethanol" to="d biomass"/>
            <rewrite from="geothermal-elect renewable" to="i geothermal"/>
            <rewrite from="crude oil CCS" to="a oil CCS"/>
            <rewrite from="regional biomassOil" to="d biomass"/>
            <rewrite from="wind-elect renewable CCS" to="g wind"/>
            <rewrite from="regional corn for ethanol CCS" to="d biomass CCS"/>
            <rewrite from="hydro-elect renewable CCS" to="f hydro"/>
            <rewrite from="regional sugar for ethanol CCS" to="d biomass CCS"/>
            <rewrite from="traded unconventional oil CCS" to="a oil CCS"/>
            <rewrite from="elect_td_ind" to=""/>
            <rewrite from="biomass CCS" to="d biomass CCS"/>
            <rewrite from="traded oil" to="a oil"/>
            <rewrite from="traded oil CCS" to="a oil CCS"/>
            <rewrite from="exotic-elect" to="j breakthrough"/>
            <rewrite from="biomass" to="d biomass"/>
            <rewrite from="nuclear-elect renewable" to="e nuclear"/>
            <rewrite from="solar-elect renewable CCS" to="h solar"/>
            <rewrite from="traded unconventional oil" to="a oil"/>
            <rewrite from="regional sugar for ethanol" to="d biomass"/>
            <rewrite from="natural gas" to="b natural gas"/>
            <rewrite from="solar-elect renewable" to="h solar"/>
            <rewrite from="traded natural gas" to="b natural gas"/>
            <rewrite from="regional sugarbeet for ethanol" to="d biomass"/>
            <rewrite from="water withdrawals" to=""/>
            <rewrite from="solar-H2 renewable" to="h solar"/>
            <rewrite from="wind-elect" to="g wind"/>
            <rewrite from="nuclear-elect renewable CCS" to="e nuclear"/>
        </level>
    </labelRewriteList>
</supplyDemandQuery>'),
  
  #QUERY_14 -- 
  "14" = list(
    "purpose-grown biomass production" = '<supplyDemandQuery title="purpose-grown biomass production">
    <axis1 name="sector">sector[@name]</axis1>
    <axis2 name="Year">physical-output[@vintage]</axis2>
    <xPath buildList="false" dataName="output" group="false" sumAll="false">*[@type=\'sector\' and (@name=\'biomass\')]//*[@type=\'output\' (:collapse:)]/
            physical-output/node()</xPath>
    <comments/>
</supplyDemandQuery>'),
  
  #QUERY_15 -- 
  "15" = list(
    "residue biomass production" = '<supplyDemandQuery title="residue biomass production">
    <axis1 name="sector">sector[@name]</axis1>
    <axis2 name="Year">physical-output[@vintage]</axis2>
    <xPath buildList="false" dataName="output" group="false" sumAll="false">*[@type=\'sector\']//output-residue-biomass/physical-output/node()</xPath>
    <comments/>
</supplyDemandQuery>'),
  
  #QUERY_16 -- 
  "16" = list(
    "MSW production" = '<resourceQuery title="MSW production">
    <axis1 name="resource">resource</axis1>
    <axis2 name="Year">output</axis2>
    <xPath buildList="false" dataName="production" group="false" sumAll="false">*[@type=\'resource\' and (@name=\'biomass\')]//output/node()</xPath>
    <comments/>
</resourceQuery>'),
  
  #QUERY_17 -- 
  "17" = list(
    "regional biomass consumption" = '<supplyDemandQuery title="regional biomass consumption">
    <axis1 name="sector">sector</axis1>
    <axis2 name="Year">demand-physical[@vintage]</axis2>
    <xPath buildList="false" dataName="input" group="false" sumAll="false">*[@type=\'sector\' and (@name=\'regional biomass\')]//*[@type=\'input\']/
            demand-physical/node()</xPath>
    <comments/>
</supplyDemandQuery>'),
  
  #QUERY_18 -- 
  "18" = list(
    "aggregated land allocation" = '<query title="aggregated land allocation">
    <axis1 name="LandLeaf">LandLeaf[@crop]</axis1>
    <axis2 name="Year">land-allocation[@year]</axis2>
    <xPath buildList="true" dataName="LandLeaf" group="false" sumAll="false">/LandNode[@name=\'root\' or @type=\'LandNode\' (:collapse:)]//land-allocation/text()</xPath>
    <comments/>
    <labelRewriteList append-values="false">
        <level name="LandLeaf">
            <rewrite from="Grassland" to="grass"/>
            <rewrite from="ProtectedUnmanagedPasture" to="pasture (other)"/>
            <rewrite from="FodderHerb" to="crops"/>
            <rewrite from="MiscCrop" to="crops"/>
            <rewrite from="PalmFruit" to="crops"/>
            <rewrite from="FiberCrop" to="crops"/>
            <rewrite from="OtherGrain" to="crops"/>
            <rewrite from="FodderGrass" to="crops"/>
            <rewrite from="ProtectedGrassland" to="grass"/>
            <rewrite from="ProtectedUnmanagedForest" to="forest (unmanaged)"/>
            <rewrite from="biomassTree" to="biomass"/>
            <rewrite from="OtherArableLand" to="otherarable"/>
            <rewrite from="Rice" to="crops"/>
            <rewrite from="UrbanLand" to="urban"/>
            <rewrite from="RockIceDesert" to="rock and desert"/>
            <rewrite from="RootTuber" to="crops"/>
            <rewrite from="Corn" to="crops"/>
            <rewrite from="OilCrop" to="crops"/>
            <rewrite from="ProtectedShrubland" to="shrubs"/>
            <rewrite from="SugarCrop" to="crops"/>
            <rewrite from="UnmanagedForest" to="forest (unmanaged)"/>
            <rewrite from="Pasture" to="pasture (grazed)"/>
            <rewrite from="Forest" to="forest (managed)"/>
            <rewrite from="biomassGrass" to="biomass"/>
            <rewrite from="Shrubland" to="shrubs"/>
            <rewrite from="UnmanagedPasture" to="pasture (other)"/>
            <rewrite from="Tundra" to="tundra"/>
            <rewrite from="Wheat" to="crops"/>
        </level>
    </labelRewriteList>
</query>')
)