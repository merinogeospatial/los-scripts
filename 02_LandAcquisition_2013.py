"""
Script calculates citywide block access scores from 2013 data with all park land in system.
06/15/16
Rob Siwiec
"""

# Import Modules
import arcpy
from arcpy import env
import os 
import collections
import statistics

""" Prepares closest route data for blocks """


def route_data(route, block):
    new_tbl = str(block)[:-4] + "_" + str(route)[:-4]
    arcpy.CopyRows_management(route, new_tbl)
    route_tbl = str(new_tbl) + "_tvw"
    arcpy.MakeTableView_management(new_tbl, route_tbl)

    arcpy.AddField_management(route_tbl, "GEOID10", "TEXT", "", "", 15, "GEOID10")
    arcpy.AddField_management(route_tbl, "SITE", "TEXT", "", "", 75, "SITE")

    expression1 = "(!Name![0:15])"
    expression2 = "(!Name![18:])"
    expression3 = "(!SITE![:-6])"
    arcpy.CalculateField_management(route_tbl, "GEOID10", expression1, "PYTHON_9.3")
    arcpy.CalculateField_management(route_tbl, "SITE", expression2, "PYTHON_9.3")
    arcpy.CalculateField_management(route_tbl, "SITE", expression3, "PYTHON_9.3")

    arcpy.AddJoin_management(block, "GEOID10", route_tbl, "GEOID10")
    field_name = str(route_tbl)[:-4]
    expression4 = "(!" + field_name + ".Total_Length!)"
    expression5 = "(!DIST!/5280)"
    arcpy.CalculateField_management(block, "DIST", expression4, "PYTHON_9.3")
    arcpy.CalculateField_management(block, "DIST", expression5, "PYTHON_9.3")
    arcpy.RemoveJoin_management(block)

    return


""" Prepares accessible route data for blocks """


def route_data_mile(route, park, block):
    new_tbl = str(block)[:-4] + "_" + str(route)[:-4]
    arcpy.CopyRows_management(route, new_tbl)
    route_tbl = str(new_tbl) + "_tvw"
    arcpy.MakeTableView_management(new_tbl, route_tbl)

    # Export table with name then do additional fields per year or whatever
    arcpy.AddField_management(route_tbl, "GEOID10", "TEXT", "", "", 15, "GEOID10")
    arcpy.AddField_management(route_tbl, "SITE", "TEXT", "", "", 75, "SITE")
    arcpy.AddField_management(route_tbl, "ACRES", "DOUBLE", "", "", "", "ACRES")
    arcpy.AddField_management(route_tbl, "POP", "LONG", "", "", "", "POP")
    arcpy.AddField_management(route_tbl, "ACRE_PP", "DOUBLE", "", "", "", "ACRE_PP")
    arcpy.AddField_management(route_tbl, "PARK_PP", "DOUBLE", "", "", "", "PARK_PP")

    expression1 = "(!Name![0:15])"
    expression2 = "(!Name![18:])"
    expression3 = "(!SITE![:-6])"
    arcpy.CalculateField_management(route_tbl, "GEOID10", expression1, "PYTHON_9.3")
    arcpy.CalculateField_management(route_tbl, "SITE", expression2, "PYTHON_9.3")
    arcpy.CalculateField_management(route_tbl, "SITE", expression3, "PYTHON_9.3")

    arcpy.AddJoin_management(route_tbl, "SITE", park, "NAME")
    field_name_1 = str(park)[:-4]
    expression4 = "(" + "!" + field_name_1 + ".MAP_ACRES!" + ")"
    arcpy.CalculateField_management(route_tbl, "ACRES", expression4, "PYTHON_9.3")
    arcpy.RemoveJoin_management(route_tbl)

    arcpy.AddJoin_management(route_tbl, "GEOID10", block, "GEOID10")
    field_name_2 = str(block)[:-4]
    expression5 = "(" + "!" + field_name_2 + ".POP!" + ")"
    arcpy.CalculateField_management(route_tbl, "POP", expression5, "PYTHON_9.3")
    arcpy.RemoveJoin_management(route_tbl)

    # Deletes rows where GEOID10 AND SITE are duplicates
    arcpy.DeleteIdentical_management(route_tbl, ["GEOID10", "SITE"])

    # summarize SITE by ACRES & POP
    site_tbl = str(route_tbl) + "_stats"
    arcpy.Statistics_analysis(route_tbl, site_tbl, [["ACRES", "MEAN"], ["POP", "SUM"]], "SITE")

    # calculate acres/person & site/person for each park
    arcpy.AddField_management(site_tbl, "ACRE_PP", "DOUBLE", "", "", "", "ACRE_PP")
    arcpy.AddField_management(site_tbl, "PARK_PP", "DOUBLE", "", "", "", "PARK_PP")
    expression6 = "(!MEAN_ACRES!/!SUM_POP!)"
    expression7 = "(1/!SUM_POP!)"
    arcpy.CalculateField_management(site_tbl, "ACRE_PP", expression6, "PYTHON_9.3")
    arcpy.CalculateField_management(site_tbl, "PARK_PP", expression7, "PYTHON_9.3")

    arcpy.AddJoin_management(route_tbl, "SITE", site_tbl, "SITE")
    expression8 = "(!" + site_tbl + ".ACRE_PP!)"
    expression9 = "(!" + site_tbl + ".PARK_PP!)"
    arcpy.CalculateField_management(route_tbl, "ACRE_PP", expression8, "PYTHON_9.3")
    arcpy.CalculateField_management(route_tbl, "PARK_PP", expression9, "PYTHON_9.3")
    arcpy.RemoveJoin_management(route_tbl)

    # Summarize route layer by GEOID
    geoid_tbl = str(route_tbl) + "_geoidStats"
    arcpy.Statistics_analysis(route_tbl, geoid_tbl, [["ACRE_PP", "SUM"], ["PARK_PP", "SUM"]], "GEOID10")

    # join back to block and calculate fields
    arcpy.AddJoin_management(block, "GEOID10", geoid_tbl, "GEOID10")
    expression10 = "(!" + geoid_tbl + ".SUM_ACRE_PP!)"
    expression11 = "(!" + geoid_tbl + ".SUM_PARK_PP!)"
    arcpy.CalculateField_management(block, "ACRE_PP", expression10, "PYTHON_9.3")
    arcpy.CalculateField_management(block, "PARK_PP", expression11, "PYTHON_9.3")
    arcpy.RemoveJoin_management(block)

    with arcpy.da.UpdateCursor(block, ["ACRE_PP", "PARK_PP"]) as cursor:
        for row in cursor:
            if row[0] is None:
                row[0] = 0
            if row[1] is None:
                row[1] = 0
            cursor.updateRow(row)
            del row
    del cursor
    return


""" Returns string with Statistic info of VALUES """


def field_range(
        d):  # Takes Dictionary {GEOID:VALUE} ex. {u'371830524061004':1.023001} - {GEOID:Distance to closest park}
    dict_list = []  # Empty list to collect VALUE of all GEOID
    for k, v in d.items():
        dict_list.append(v)  # Appends each VALUE to list
    dict_list.sort()  # Sorts VALUES in ascending order "LO to HIGH"
    list_len = len(dict_list)  # Gets length of list
    last = list_len - 1  # Gets highest value and rounds
    hi = round(dict_list[last], 3)
    lo = round(dict_list[0], 3)  # Gets lowest value and rounds
    list_mean = round(statistics.mean(dict_list), 10)  # Mean of VALUES
    list_sd = round(statistics.stdev(dict_list), 10)  # Standard deviation of VALUES
    list_line = "Mean Distance: " + str(list_mean) + ',' + "SD Distance: " + \
                str(list_sd) + ',' + "Length: " + str(list_len) + ',' + \
                "Highest Value: " + str(hi) + ',' + "Lowest Value: " + str(lo)
    return list_line


"""Returns dictionary {SD:[LO,HI]} """


def get_ranges(d):  # Takes Dictionary {GEOID:VALUE} ex. {u'371830524061004':1.02} - {GEOID:Distance to closest park}
    dict_list = []  # Empty list to collect VALUE of all GEOID
    for key, value in d.items():
        dict_list.append(value)  # Appends each VALUE to list
    dict_list.sort()  # Sorts VALUES in ascending order "LO to HIGH"
    list_len = len(dict_list)  # Gets length of list
    last = list_len - 1  # Gets highest value and rounds
    hi = round(dict_list[last], 9)
    list_mean = round(statistics.mean(dict_list), 9)  # Mean of VALUES                                round(x,3) default
    list_sd = round(statistics.stdev(dict_list), 9)  # Standard deviation of VALUES                  round(x,3) default
    ranges = {}  # Empty dictionary to hold {RANGENAME:[LO,HI]}
    arc_sd = list_sd / 2  # Half of sd
    mean_hi = round(((list_mean + arc_sd) - 0.000000001),
                    9)  # Value at Half SD above mean                   round(x,6) default
    mean_lo = round(((list_mean - arc_sd) + 0.000000001),
                    9)  # Value at Half SD below mean                   round(x,6) default
    ranges['0'] = [mean_lo, mean_hi]  # Range about mean
    upper_limit = hi  # Highest VALUE
    x = list_mean + arc_sd  # SD increment above mean
    y = 1  # Set range name above mean
    while x < upper_limit:  # While loop to create ranges above mean
        sd = x + list_sd  # Range for 1 SD above 1/2 sd about mean
        sd_hi = round((sd - 0.000000001), 9)  # Hi value of range                             round(x,6) default
        sd_lo = round((x + 0.000000001), 9)  # Lo value of range                             round(x,6) default
        range_name = str(y)  # Range name based on values about mean
        ranges[range_name] = [sd_lo, sd_hi]  # Create range{key:value}
        x += list_sd  # Increment range
        y += 1  # Increment range name
    x = list_mean - arc_sd  # SD increment below mean
    y = -1  # Set range name below mean
    while x > 0:  # While loop to create ranges below mean
        sd = x - list_sd  # Range for 1 SD below 1/2 sd about mean
        sd_hi = round((sd + 0.000000001),
                      9)  # Lo value of range                             round(x,6) default
        sd_lo = round((x - 0.000000001), 9)  # Hi value of range                             round(x,6) default
        if sd_hi <= 0:
            sd_hi = 0  # Values do not go below zero
        range_name = str(y)  # Range name based on values about mean
        ranges[range_name] = [sd_hi, sd_lo]  # Create range{key:value}
        x -= list_sd  # Increment range
        y -= 1  # Increment range name
    return ranges


"""Returns dictionary {GEOID:SD} """


def get_sd(d, baseline=None):  # dictionary has {GEOID:VALUE,...}, baseline is for 2013 ranges dictionary -
    # result of getRanges(), otherwise it creates new ranges
    std_dev = {}  # Empty dictionary to hold {GEOID:SD range}
    if baseline is None:
        list_breaks = get_ranges(d)  # Call getBreaks function. Returns dictionary. Has {RANGE:[lo,hi],...} for values
    else:
        list_breaks = baseline
    sd_range = []  # Range of SD [-1,0,1,2,3...]
    for r, l in list_breaks.items():
        if r not in sd_range:
            sd_range.append(r)
    sd_range.sort()
    sd_len = len(sd_range)
    hi_index = sd_len - 1
    hi_sd = sd_range[hi_index]
    for k, v in d.items():  # Iterate through getSD function dictionary
        for key, value in list_breaks.items():  # Iterate through getBreaks dictionary
            if list_breaks[key][0] <= v <= list_breaks[key][1]:  # If VALUE falls between range,
                std_dev[k] = int(key)  # populate dictionary with {GEOID:SD} based on that range
            elif v > list_breaks[hi_sd][1]:  # Check if value is higher than highest range
                # (may happen with changes to values annually)
                std_dev[k] = int(hi_sd)
    return std_dev


""" Returns dictionary with score and associated SD value - {SCORE:[SD]}
    ex. {1:-1,2:0,3:1,4:2,5:[3,4,5]} """


def sd_score(range_breaks, order):  # dictionary has {GEOID:SD}, rangeBreaks has {SD:[LO,HI]}
    sd_range = []
    for k, v in range_breaks.items():
        if k not in sd_range:
            k_int = int(k)
            sd_range.append(k_int)
    sd_range.sort()
    a = len(sd_range)
    score_dict = {}
    if order == 0:  # Ascending Order - distance
        if a <= 5:
            for sd in sd_range:
                i = sd_range.index(sd)
                i += 1
                score_dict[i] = sd
        else:
            hi_list = []
            for sd in sd_range:
                i = sd_range.index(sd)
                i += 1
                if i >= 5:
                    hi_list.append(sd)
                else:
                    score_dict[i] = sd
            score_dict[5] = hi_list
    elif order == 1:  # Descending Order - parks, acres
        if a <= 5:
            for sd in sd_range:
                i = sd_range.index(sd)
                i = a - i
                score_dict[i] = sd
        else:
            hi_list = []
            for sd in sd_range:
                i = sd_range.index(sd)
                if i > 3:
                    hi_list.append(sd)
            score_dict[1] = hi_list
            for sd in sd_range:
                i = sd_range.index(sd)
                i = 5 - i
                if i > 1:
                    score_dict[i] = sd
    return score_dict


def block_score(sd, score, order):
    # sd = {GEOID:SD}, score = {SCORE:[SD]} #
    # Convert SD to score for blocks - blockScore(getSD, sdScore, ascend/descend)
    # !! ORDER argument must match results of sdScore() function with same ORDER !!#
    val_range = []  # holds range of score values [-1,0,1,2,3,4,5]
    geo_score = {}  # Dictionary {GEOID:SCORE}
    if order == 0:  # Ascending Order for Score
        in_list = score.get(5)  # Get list of highest score values, ex. Score 5 = [3,4,5]
        if type(in_list) is int:
            val_range.append(in_list)
        else:
            for item in in_list:
                val_range.append(item)
        for k, v in score.items():  # Append other score values to new list to get a list of all score values
            if v not in val_range and v != in_list:
                val_range.append(v)
    if order == 1:  # Descending Order for Score
        in_list = score.get(1)  # Get list of lowest score values, ex. Score 1 = [3,4,5]
        if type(in_list) is int:
            val_range.append(in_list)
        else:
            for item in in_list:
                val_range.append(item)
        for k, v in score.items():  # Append other score values to new list to get a list of all score values
            if v not in val_range and v != in_list:
                val_range.append(v)
    val_range.sort()
    val_range = len(val_range)
    if val_range > 5:
        if order == 0:
            for k, v in sd.items():
                if v in score[5]:
                    geo_score[k] = 5
                else:
                    for key, value in score.items():
                        if v == score[key]:
                            geo_score[k] = key
        if order == 1:
            for k, v in sd.items():
                if v in score[1]:
                    geo_score[k] = 1
                else:
                    for key, value in score.items():
                        if v == score[key]:
                            geo_score[k] = key
    if val_range <= 5:
        for k, v in sd.items():
            for key, value in score.items():
                if v == score[key]:
                    geo_score[k] = key
    return geo_score


def bg_values(count_list):  # Determine weighted values for BG - bgValues(count list, blockScore, ascend/descend)
    count_list.sort()  # Sort list
    counter_dict = collections.Counter(count_list)  # Returns dictionary of {VALUE:FREQUENCY}
    block_sum = []  # Empty list to hold frequency totals
    block_group_score = []  # Empty list to hold scores in BG
    for k, v in counter_dict.items():  # Iterate through counter dictionary to create list of freq totals
        block_sum.append(float(v))
        block_group_score.append((float(k)) * (float(v)))
    blocks = sum(block_sum)  # total blocks in bg (Sum of freq totals)
    score_sum = sum(block_group_score)  # Sum of block scores in bg
    score_total = score_sum / blocks  # Average score of blocks in bg
    return score_total


#####################################################################################
arcpy.env.workspace = arcpy.GetParameterAsText(0)
# add a default using relative paths
la_gdb = arcpy.env.workspace
env.overwriteOutput = True 
run_bg = "YES"
#####################################################################################

# Variables
# files
blocks = arcpy.GetParameterAsText(1)
parks = arcpy.GetParameterAsText(2)
mile_routes = arcpy.GetParameterAsText(3)
closest_routes = arcpy.GetParameterAsText(4)
###### ABOVE FROM LOS BASELINE ######
blocks_current_year = arcpy.GetParameterAsText(5)
parks_current_year = arcpy.GetParameterAsText(6)
mile_routes_current_year = arcpy.GetParameterAsText(7)
closest_routes_current_year = arcpy.GetParameterAsText(8)
block_group = arcpy.GetParameterAsText(9)

# layers
blocks_lyr = "BLOCKS_2013_lyr"
parks_lyr = "Parks_2013_lyr"
mile_routes_lyr = "MileRoutes_2013_lyr"
closest_routes_lyr = "ClosestRoutes_2013_lyr"
blocks_current_year_lyr = "BLOCKS_2013_LA_lyr"
parks_current_year_lyr = "Parks_2013_LA_lyr"
mile_routes_current_year_lyr = "MileRoutes_2013_LA_lyr"
closest_routes_current_year_lyr = "ClosestRoutes_2013_LA_lyr"
block_group_lyr = "BLOCKGROUP_2013_LA_lyr"

# Create layers
arcpy.MakeFeatureLayer_management(blocks, blocks_lyr)
arcpy.MakeFeatureLayer_management(parks, parks_lyr)
arcpy.MakeFeatureLayer_management(mile_routes, mile_routes_lyr)
arcpy.MakeFeatureLayer_management(closest_routes, closest_routes_lyr)
arcpy.MakeFeatureLayer_management(blocks_current_year, blocks_current_year_lyr)
arcpy.MakeFeatureLayer_management(parks_current_year, parks_current_year_lyr)
arcpy.MakeFeatureLayer_management(mile_routes_lyr, mile_routes_current_year_lyr)
arcpy.MakeFeatureLayer_management(closest_routes_current_year, closest_routes_current_year_lyr)
arcpy.MakeFeatureLayer_management(block_group, block_group_lyr)

# Route Prep
# Check if records have values so route prep is not duplicated if previously run
dist_records = []
with arcpy.da.SearchCursor(blocks_lyr, "DIST") as cursor:
    for row in cursor:
        dist_records.append(row[0])
        del row
del cursor
if None in dist_records:
    route_data(closest_routes_lyr, blocks_lyr)
    route_data_mile(mile_routes_lyr, parks_lyr, blocks_lyr)
dist_records_current_year = []
with arcpy.da.SearchCursor(blocks_current_year_lyr, "DIST") as cursor:
    for row in cursor:
        dist_records_current_year.append(row[0])
        del row
del cursor
if None in dist_records_current_year:
    route_data(closest_routes_current_year_lyr, blocks_current_year_lyr)
    route_data_mile(mile_routes_current_year_lyr, parks_current_year_lyr, blocks_current_year_lyr)

# Census blocks BASELINE
"""Create empty dictionaries to hold {GEOID:Field}values"""
dist_list = {}  # {GEOID:Distance}
acre_list = {}  # {GEOID:Acre/Mile}
park_list = {}  # {GEOID:Park/Mile}

"""Create empty list to hold all geoid"""
geoid_list = []

"""Add all geoid to list"""
with arcpy.da.SearchCursor(blocks_lyr, "GEOID10") as cursor:
    for row in cursor:
        if row[0] not in geoid_list:
            geoid_list.append(row[0])
        del row
del cursor

# Pull Values
field_names = ["GEOID10", "DIST", "ACRE_PP", "PARK_PP"]  # Field names list for search cursor
with arcpy.da.SearchCursor(blocks_lyr, field_names) as cursor:  # Search cursor in census block layer
    for row in cursor:  # For each row, populate dictionary from field names list
        dist_list[row[0]] = row[1]  # Populate distList dictionary {GEOID:Closest Park VALUE}
        acre_list[row[0]] = row[2]  # Populate acreList dictionary {GEOID:Acreage VALUE}
        park_list[row[0]] = row[3]  # Populate parkList dictionary {GEOID:ParkCount VALUE}
        del row
del cursor

# Run Stats
arcpy.AddMessage("Running stats... this may take a moment...")
dist_range = get_ranges(dist_list)  # Call getRanges function to get {SD:[LO,HI]} for distance
acre_range = get_ranges(acre_list)  # Call getRanges function to get {SD:[LO,HI]} for acreage
park_range = get_ranges(park_list)  # Call getRanges function to get {SD:[LO,HI]} for parks

# Census blocks CURRENT YEAR
"""Create empty list for total population values"""
etj_pop = []
with arcpy.da.SearchCursor(blocks_current_year_lyr, "POP") as cursor:
    for row in cursor:
        etj_pop.append(row[0])
        del row
del cursor
total_pop = sum(etj_pop)

"""Create empty dictionaries to hold {GEOID:Field}values"""
dist_list_current_year = {}  # {GEOID:Distance}
acre_list_current_year = {}  # {GEOID:Acre/Mile}
park_list_current_year = {}  # {GEOID:Park/Mile}

"""Create empty list to hold all geoid"""
geoid_list_current_year = []

"""Add all geoid to list"""
with arcpy.da.SearchCursor(blocks_current_year_lyr, "GEOID10") as cursor:
    for row in cursor:
        if row[0] not in geoid_list_current_year:
            geoid_list_current_year.append(row[0])
        del row
del cursor
# Pull Values
field_names_current_year = ["GEOID10", "DIST", "ACRE_PP", "PARK_PP"]  # Field names list for search cursor
with arcpy.da.SearchCursor(blocks_current_year_lyr, field_names_current_year) as cursor:  # Search cursor in cb layer
    for row in cursor:  # For each row, populate dictionary from field names list
        dist_list_current_year[row[0]] = row[1]  # Populate distList dictionary {GEOID:Closest Park VALUE}
        acre_list_current_year[row[0]] = row[2]  # Populate acreList dictionary {GEOID:Acreage VALUE}
        park_list_current_year[row[0]] = row[3]  # Populate parkList dictionary {GEOID:ParkCount VALUE}
        del row
del cursor

# Run Stats
dist_sd_current_year = get_sd(dist_list_current_year, dist_range)  # Call getSD to get {GEOID:SD} for distance
acre_sd_current_year = get_sd(acre_list_current_year, acre_range)  # Call getSD to get {GEOID:SD} for acreage
park_sd_current_year = get_sd(park_list_current_year, park_range)  # Call getSD to get {GEOID:SD} for parks

dist_score_current_year = sd_score(dist_range, 0)  # Call sdScore to get {SCORE:SD} for distance
acre_score_current_year = sd_score(acre_range, 1)  # Call sdScore to get {SCORE:SD} for acreage
park_score_current_year = sd_score(park_range, 1)  # Call sdScore to get {SCORE:SD} for parks

# Call blockScore function to get {GEOID:SCORE} for distance
dist_blocks_current_year = block_score(dist_sd_current_year, dist_score_current_year, 0)
# Call blockScore function to get {GEOID:SCORE} for acreage
acre_blocks_current_year = block_score(acre_sd_current_year, acre_score_current_year, 1)
# Call blockScore function to get {GEOID:SCORE} for parks
park_blocks_current_year = block_score(park_sd_current_year, park_score_current_year, 1)

# Set Scores
# fields in census block layer
cb_fields = ["GEOID10", "DSCORE_2013", "ASCORE_2013", "PSCORE_2013", "TOTAL_SCORE", "POP_WGT", "WGT_SCORE", "POP"]
with arcpy.da.UpdateCursor(blocks_current_year_lyr, cb_fields) as cursor:  # Update cursor for census block layer
    for row in cursor:  # For each row, populate fields from dictionary
        for k, v in dist_blocks_current_year.items():  # Update field with SCORE from blockScore function
            if row[0] == k:
                row[1] = v
        for k, v in acre_blocks_current_year.items():  # Update field with SCORE from blockScore function
            if row[0] == k:
                row[2] = v
        for k, v in park_blocks_current_year.items():  # Update field with SCORE from blockScore function
            if row[0] == k:
                row[3] = v
        row[4] = (row[1] + row[2] + row[3])  # new
        row[5] = (float(row[7]) / float(total_pop))  # new
        row[6] = ((row[4] * row[5]) * 100)  # new
        cursor.updateRow(row)
        del row
del cursor

# Census Block Groups
if run_bg == "YES":
    bg_list = []
    with arcpy.da.SearchCursor(block_group_lyr, "GEOID") as cursor:
        for row in cursor:
            bg_list.append(row[0])
            del row
    del cursor

    bg_list.sort()
    for bg in bg_list:
        q1 = """ GEOID = """ + "\'" + str(bg) + "\'"
        arcpy.SelectLayerByAttribute_management(block_group_lyr, "NEW_SELECTION", q1)
        arcpy.SelectLayerByLocation_management(blocks_current_year_lyr, "WITHIN", block_group_lyr,
                                               selection_type="NEW_SELECTION")
        block_output = "BG_" + str(bg)
        arcpy.MakeFeatureLayer_management(blocks_current_year_lyr, block_output)

        bg_pop = []  # Population of each Block in Block Group

        dist_count = []  # Create empty list to hold block scores for distance
        acre_count = []  # Create empty list to hold block scores for acreage
        park_count = []  # Create empty list to hold block scores for parks

        fields = ["GEOID10", "POP"]
        with arcpy.da.SearchCursor(block_output, fields) as cursor:  # Search cursor in Census Block layer
            for row in cursor:
                bg_pop.append(row[1])  # Add POP value for each block to list
                for k, v in dist_blocks_current_year.items():
                    if row[0] == k:
                        dist_count.append(v)  # Add Score value to distance list
                for k, v in acre_blocks_current_year.items():
                    if row[0] == k:
                        acre_count.append(v)  # Add Score value to acreage list
                for k, v in park_blocks_current_year.items():
                    if row[0] == k:
                        park_count.append(v)  # Add Score value to parks list
                del row
        del cursor

        bg_pop_sum = sum(bg_pop)  # total population for block group

        dist_total = bg_values(dist_count)  # Blocks in bg for distance
        park_total = bg_values(park_count)  # Blocks in bg for parks
        acre_total = bg_values(acre_count)  # Blocks in bg for acreage

        bg_fields = ["POP_2013", "DSCORE_2013", "ASCORE_2013", "PSCORE_2013", "TOTAL_SCORE", "POP_WGT", "WGT_SCORE"]
        exp_2 = """ GEOID = """ + "\'" + str(bg) + "\'"
        with arcpy.da.UpdateCursor(block_group_lyr, bg_fields, where_clause=exp_2) as cursor:
            for row in cursor:
                row[0] = bg_pop_sum
                row[1] = dist_total
                row[2] = acre_total
                row[3] = park_total
                row[4] = (row[1] + row[2] + row[3])
                row[5] = (float(row[0]) / float(total_pop))
                row[6] = ((row[4] * row[5]) * 100)
                cursor.updateRow(row)
                del row
        del cursor
arcpy.AddMessage("COMPLETED - CHECK THE BLOCK & BLOCK GROUP LAYERS")


