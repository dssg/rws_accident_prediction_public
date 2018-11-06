import functools
import pandas as pd
import sys
import re
from utils.misc_utils import pandas_to_db
    
def column_name(column_name):
    def wrapped(fn):
        @functools.wraps(fn)
        def wrapped_f(*args, **kwargs):
            return fn(*args, **kwargs)
        wrapped_f.column_name = column_name
        return wrapped_f
    return wrapped

# commonly used aggregation methods
def get_max(self, series_hectopunt, val_if_null):
    if series_hectopunt.notnull().sum()>0:
        return series_hectopunt.max()
    else:
        return val_if_null
    
def get_min(self, series_hectopunt, val_if_null):
    if series_hectopunt.notnull().sum()>0:
        return series_hectopunt.min()
    else:
        return val_if_null
    
def get_mean(self, series_hectopunt, val_if_null):
    if series_hectopunt.notnull().sum()>0:
        return series_hectopunt.mean()
    else:
        return val_if_null
    
def get_total(self, series_hectopunt):
    if series_hectopunt.notnull().sum()>0:
        return series_hectopunt.sum()
    else:
        return 0
    
def get_mode_dropna(self, series_hectopunt):
    count_vals = series_hectopunt.value_counts(dropna=True)
    if count_vals.empty:
        return None
    else:
        common_value = count_vals.index[0]
        if not common_value:
            return None
        else:
            if isinstance(common_value, str):
                #non-alpha numeric characters can cause error when inserting data to PSQL
                # Therefore we need to remove them
                output = re.sub('[^0-9a-zA-Z]+', '', common_value) 
                return output
                
def get_count_per_km(self, series_hectopunt):
    if series_hectopunt.notnull().sum()>0:
        num_km = series_hectopunt.shape[0]/float(10.0)#number of kilometers
        return series_hectopunt.count()/num_km
    else:
        return 0
    
def get_road_type_perc(self, series_hectopunt, letter):
    '''percentage of letter road_type'''         
    return series_hectopunt[series_hectopunt==letter].shape[0]/float(series_hectopunt.shape[0])

def has_value(self, series_hectopunt, value):
    for c in series_hectopunt:
        if c==value:
            return 1
        else:
            continue
    return 0


class HectopuntenFeatureFactory(object):
    def __init__(self, hectopunten_table, hectopunten_mapping_table, conn,
                hectopunten_rollup_table):
        '''
        
        Level of Aggregation in space depends on the mapping table
        
        Guidelines to create new features:
            - Each Feature should be a new method
            - Name of the function will become name of the feature
            - Use column_name decorator to map which column of hectopunten does
              the feature apply to
            - Each method expects a group of hectopuntens and returns one value for it.
            - If a feature requires multiple columns, @column_name can be custom and for
            our purpose be same as the name of eventual feature/method.
        
        
        Developers won't need to hamper with the remaining part of the code.
        Just interact with the methods in the class.
        
        External will only interact with the Driver function.
        '''
        
        ## for now taking it directly
        q = 'select * from {0} as h\
        left join \
        {1} as s \
        on h.hectokey = s.hectokey;'.format(hectopunten_rollup_table, hectopunten_mapping_table)
        self.linked_hectopunten = pd.read_sql(q,con=conn)
        
##### Number of Lanes
    @column_name('num_lanes_min')
    def min_number_lanes_avgxseg_num(self, series_hectopunt):
        '''assumes it gets the feature for a series of hectopuntens and returns one value
        name of the function becomes the method'''
        return pd.np.mean(series_hectopunt)
    
    @column_name('num_lanes_max')
    def max_number_lanes_avgxseg_num(self, series_hectopunt):
        '''assumes it gets the feature for a series of hectopuntens and returns one value'''
        return pd.np.mean(series_hectopunt)
    
 

##### Road Number
    @column_name('road_num')
    def includes_road_number_12_cat(self, series_hectopunt):
        return has_value(self, series_hectopunt, 12)
    
    @column_name('road_num')
    def includes_road_number_1_cat(self, series_hectopunt):
        return has_value(self, series_hectopunt, 1)
    
    @column_name('road_num')
    def includes_road_number_28_cat(self, series_hectopunt):
        return has_value(self, series_hectopunt, 28)
    
    @column_name('road_num')
    def includes_road_number_27_cat(self, series_hectopunt):
        return has_value(self, series_hectopunt, 27)
    
    @column_name('road_num')
    def includes_road_number_2_cat(self, series_hectopunt):
        return has_value(self, series_hectopunt, 2)


########SPEED LIMIT

    @column_name('speedlim_6_19')
    def speedlim_day_maxxseg_num(self, series_hectopunt):
        '''max speed limit daytime'''
        return get_max(self, series_hectopunt, None)

    @column_name('speedlim_19_6')
    def speedlim_night_maxxseg_num(self, series_hectopunt):
        '''max speed limit night'''
        return get_max(self, series_hectopunt, None)         

    @column_name('speedlim_6_19')
    def speedlim_day_avgxseg_num(self, series_hectopunt):
        '''mean speed limit daytime'''
        return get_mean(self, series_hectopunt, None)       

    @column_name('speedlim_19_6')
    def speedlim_night_avgxseg_num(self, series_hectopunt):
        '''mean speed limit at night'''
        return get_mean(self, series_hectopunt, None)         

    @column_name('speedlim_6_19')
    def speedlim_day_minxseg_num(self, series_hectopunt):
        '''min speed limit daytime'''
        return get_min(self, series_hectopunt, None)        

    @column_name('speedlim_19_6')
    def speedlim_night_minxseg_num(self, series_hectopunt):
        '''min speed limit night'''
        return get_min(self, series_hectopunt, None)    
     
    #removed because mostly null    
    #@column_name('advisory_speed')
    #def advisory_speed_maxxseg_num(self, series_hectopunt):
        #'''maximum advised speed'''
        #return get_max(self, series_hectopunt, None)

    #@column_name('advisory_speed')
    #def advisory_speed_avgxseg_num(self, series_hectopunt):
        #'''average advised speed'''
        #return get_mean(self, series_hectopunt, None)    

## ON-RAMP and OFF-RAMP
    @column_name('on_ramp_convergentie')
    def entrances_perkm_usingconvergentie_num(self,series_hectopunt):
        '''number of entrances per km'''
        return get_count_per_km(self, series_hectopunt)
        
    @column_name('on_ramp_divergentie')
    def exits_perkm_usingdivergentie_num(self,series_hectopunt):
        '''number of entrances per km'''
        return get_count_per_km(self, series_hectopunt)


    @column_name('distance_to_bst_code_opr')
    def entrances_perkm_usingbstcode_num(self, series_hectopunt):
        '''number of entrances per km'''
        return get_count_per_km(self, series_hectopunt)       

    @column_name('distance_to_bst_code_afr')
    def exits_perkm_usingbstcode_num(self, series_hectopunt):
        '''number of exits per km'''
        return get_count_per_km(self, series_hectopunt) 

    @column_name('on_ramp_convergentie')
    def entrance_type_mode_cat(self, series_hectopunt):
        '''Most common entrance type'''
        return get_mode_dropna(self, series_hectopunt)

    @column_name('off_ramp_divergentie')
    def exit_type_mode_cat(self, series_hectopunt):
        '''Most common exit type'''
        return get_mode_dropna(self, series_hectopunt)

   
## ASPHALT
    @column_name('asphalt')
    def asphalt_mode_cat(self, series_hectopunt):
        '''Most common asphalt type'''
        return get_mode_dropna(self, series_hectopunt)
    

## TUNNEL
    # removing tunnel since all nulls
    #@column_name('tunnel')
    #def tunnel_mode_cat(self, series_hectopunt):
        #'''Most common tunnel type IF there's one'''
        #count_vals = series_hectopunt.value_counts(dropna=True)
        #if count_vals.empty:
            #return None
        #else:
            #common_value = count_vals.index[0]
            #if not common_value:
                #return None
            #else:
                #return common_value

## Lighting
    @column_name('lighting')
    def lighting_mode_cat(self, series_hectopunt):
        '''Most common  type of lighting.  Note that there are many roads that do not have any type of lighting'''
        series_hectopunt = series_hectopunt.fillna("No Lighting")
        return series_hectopunt.value_counts(dropna=False).index[0]
    
    @column_name('lamp_post')
    def lamppost_perkm_num(self, series_hectopunt):
        '''Number of lamp posts present per km'''
        return get_count_per_km(self, series_hectopunt)

    @column_name('lighting')
    def has_lighting_cat(self, series_hectopunt):
        '''Return True, if there's some info about lighting'''
        return get_mode_dropna(self, series_hectopunt)
            
 ## CURVE
    @column_name('curve')
    def curvature_minxseg_num(self, series_hectopunt):
        '''Min. curve: note that the curve is sharper as it is closer to zero '''
        return get_min(self, series_hectopunt, 1e6)

    @column_name('curve')
    def curvature_avgxseg_num(self, series_hectopunt):
        '''mean curve: note that the curve is sharper as it is closer to zero '''
        return get_mean(self, series_hectopunt, 1e6)

    @column_name('curve')
    def curvature_maxxseg_num(self, series_hectopunt):
        '''Max. curve: note that the curve is sharper as it is closer to zero '''
        return get_max(self, series_hectopunt, 1e6)
###TILT
    @column_name('tilt')
    def tilt_minxseg_num(self, series_hectopunt):
        '''Min. tilt: note that the tilt level is higher as it is closer to zero '''
        return get_min(self, series_hectopunt, 0)

    @column_name('tilt')
    def tilt_avgxseg_num(self, series_hectopunt):
        '''Mean. tilt: note that the tilt level is higher as it is closer to zero '''
        return get_mean(self, series_hectopunt, 0)

    @column_name('tilt')
    def tilt_maxxseg_num(self, series_hectopunt):
        '''Max. tilt: note that the tilt level is higher as it is closer to zero '''
        return get_mean(self, series_hectopunt,0)


## Alphalt Age 
    @column_name('asphalt_age')
    def asphalt_age_avgxseg_num(self, series_hectopunt):
        '''Average age of the asphalt within the segment (units unknown) '''
        return get_mean(self, series_hectopunt, None)
        
    @column_name('asphalt_age')
    def asphalt_age_maxxseg_num(self, series_hectopunt):
        '''Maximum age of the asphalt within the segment (units unknown) '''
        return get_max(self, series_hectopunt, None)            
    
    @column_name('asphalt_age')
    def asphalt_age_minxseg_num(self, series_hectopunt):
        '''Minimum age of the asphalt within the segment (units unknown) '''
        return get_min(self, series_hectopunt, None)
        
        
 ## Number of billboards   
    @column_name('distance_to_billboard')
    def billboards_perkm_num(self, series_hectopunt):
        '''Number of hectopunten with billboards normalized by segment length in km '''
        return get_count_per_km(self, series_hectopunt)
        
## Number of trees
    @column_name('trees_within50m')
    def trees_within50m_maxxseg_num(self, series_hectopunt):
        '''Max number of trees within 50m on a single hectopunten '''
        return get_max(self, series_hectopunt, None)
        
    @column_name('trees_within50m')
    def trees_within50m_minxseg_num(self, series_hectopunt):
        '''Min number of trees within 50m on a single hectopunten '''
        return get_min(self, series_hectopunt, None)
        
    @column_name('trees_within50m')
    def trees_within50m_avgxseg_num(self, series_hectopunt):
        '''Avg number of trees within 50m on a single hectopunten '''
        return get_mean(self, series_hectopunt, None) 
        
## Overtaking allowed
    @column_name('overtaking_allowed')
    def hecto_withovertakingallowed_perkm_num(self, series_hectopunt):
        '''Number of hectopunten where overtaking is allowed normalized by segment length '''
        return get_count_per_km(self, series_hectopunt)
        
 ## distance to features
    @column_name('distance_from_road_to_sound_barrier')
    def distanceto_soundbarrier_perkm_num(self, series_hectopunt):
        '''Average distance to sound barrier '''
        return get_mean(self, series_hectopunt, None)
        
    @column_name('distance_to_railway_crossing')
    def hectowith_railwaycrossing_totalxseg_num(self, series_hectopunt):
        '''Has railway crossing '''
        return get_total(self, series_hectopunt)
        
    @column_name('distance_to_parking')
    def hectowith_parking_totalxseg_num(self, series_hectopunt):
        '''Has parking '''
        return get_total(self, series_hectopunt)
        
    @column_name('distance_to_intersection')
    def hectowith_intersection_totalxseg_num(self, series_hectopunt):
        '''Has intersection '''
        return get_total(self, series_hectopunt)
        
    @column_name('distance_to_lane_widening')
    def hectowith_lanewidening_totalxseg_num(self, series_hectopunt):
        '''Has lane widening'''
        return get_total(self, series_hectopunt)
        
    @column_name('distance_to_lane_narrowing')
    def hectowith_lanenarrowing_totalxseg_num(self, series_hectopunt):
        '''Average distance to sound barrier '''
        return get_total(self, series_hectopunt)

## direction (which gps direction road flow is occuring)
    @column_name('compass_direction_degrees')
    def compass_dirdeg_avgxseg_num(self, series_hectopunt):
        '''Compass direction in degrees '''
        return get_mean(self, series_hectopunt, None)
        
    @column_name('compass_direction_category')
    def compass_dir_mode_cat(self, series_hectopunt):
        '''Most common compass direction (N,Z,O,W (corresponds to N,S,E,W)) '''
        return get_mode_dropna(self, series_hectopunt)
            
# Merge lanes
    @column_name('merge_lane_type')
    def hectowith_mergelanes_mode_cat(self, series_hectopunt):
        '''Number of hectopunten with a marge lane '''
        return get_mode_dropna(self, series_hectopunt)

# Pre 2016 Hectopunten Features
    @column_name('accidents_pre2016')
    def accidents_pre2016_totalxseg_num(self, series_hectopunt):
        '''Number of accidents at that location from 2012 to 2016'''
        return get_total(self, series_hectopunt)

    @column_name('dod_accidents_pre2016')
    def dod_accidents_pre2016_totalxseg_num(self, series_hectopunt):
        '''Number of accidents at that location from 2012 to 2016'''
        return get_total(self, series_hectopunt)

    @column_name('leh_accidents_pre2016')
    def leh_accidents_pre2016_totalxseg_num(self, series_hectopunt):
        '''Number of accidents at that location from 2012 to 2016'''
        return get_total(self, series_hectopunt)

    @column_name('lzh_accidents_pre2016')
    def lzh_accidents_pre2016_totalxseg_num(self, series_hectopunt):
        '''Number of accidents at that location from 2012 to 2016'''
        return get_total(self, series_hectopunt)

    @column_name('ums_accidents_pre2016')
    def ums_accidents_pre2016_totalxseg_num(self, series_hectopunt):
        '''Number of accidents at that location from 2012 to 2016'''
        return get_total(self, series_hectopunt)

##### Feature Combinations    
    ## Below is a proof of concept of using two hectopunten columns
    ## to create a new feature.
    #@column_name('num_lanes_max_minus_min')
    #def road_num_leans_max_minus_min(self, series_hectopunt):
        #group = self.linked_hectopunten.loc[series_hectopunt.index]
        #max_lane = group.num_lanes_max.max()
        #min_lane = group.num_lanes_min.min()
        #difference = max_lane - min_lane
        #return difference
        
    @column_name('onramps_convergentie_or_bstcode_totalxseg_num')
    def onramps_convergentie_or_bstcodeopr_totalxseg_num(self, series_hectopunt):
        group = self.linked_hectopunten.loc[series_hectopunt.index]
        return (group.distance_to_on_ramp_convergentie.notnull() | group.distance_to_bst_code_opr.notnull()).sum()
    
    @column_name('onramps_convergentie_or_bstcodeopr_perkm_num')
    def onramps_convergentie_or_bstcodeopr_perkm_num(self, series_hectopunt):
        group = self.linked_hectopunten.loc[series_hectopunt.index]
        num_km = series_hectopunt.shape[0]/float(10.0)#number of kilometers
        return (group.distance_to_on_ramp_convergentie.notnull() | group.distance_to_bst_code_opr.notnull()).sum()/num_km
    
    
    @column_name('curvyroad_and_onramp_totalxseg_num')
    def curvyroad_and_onramp_totalxseg_num(self, series_hectopunt):
        group = self.linked_hectopunten.loc[series_hectopunt.index]
        
        has_ramp = group.distance_to_on_ramp_convergentie.notnull() | group.distance_to_bst_code_opr.notnull()
        
        has_curve = group.curve.notnull()
        
        has_ramp_and_curve = (has_ramp & has_curve).sum();
        if has_ramp_and_curve:
            return has_ramp_and_curve
        else:
            return 0
        
    @column_name('offramps_divergentie_or_bstcodeafr_totalxseg_num')
    def offramps_divergentie_or_bstcodeafr_totalxseg_num(self, series_hectopunt):
        group = self.linked_hectopunten.loc[series_hectopunt.index]
        return (group.distance_to_off_ramp_divergentie.notnull() | group.distance_to_bst_code_afr.notnull()).sum()
    
    @column_name('offramps_divergentie_or_bstcodeafr_perkm_num')
    def offramps_divergentie_or_bstcodeafr_perkm_num(self, series_hectopunt):
        group = self.linked_hectopunten.loc[series_hectopunt.index]
        num_km = series_hectopunt.shape[0]/float(10.0)#number of kilometers
        return (group.distance_to_off_ramp_divergentie.notnull() | group.distance_to_bst_code_afr.notnull()).sum()/num_km
    
    
    @column_name('curvyroad_and_offramp_totalxseg_num')
    def curvyroad_and_offramp_totalxseg_num(self, series_hectopunt):
        group = self.linked_hectopunten.loc[series_hectopunt.index]
        
        has_ramp = group.distance_to_off_ramp_divergentie.notnull() | group.distance_to_bst_code_afr.notnull()
        
        has_curve = group.curve.notnull()
        
        has_ramp_and_curve = (has_ramp & has_curve).sum();
        if has_ramp_and_curve:
            return has_ramp_and_curve
        else:
            return 0
    

##### normalized road_type count in descending order 
    @column_name('road_type')
    def road_type_n_perc_num(self, series_hectopunt):
        '''percentage of 'n' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "n")

    @column_name('road_type')
    def road_type_m_perc_num(self, series_hectopunt):
        '''perc of 'm' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "m")

 
    @column_name('road_type')
    def road_type_a_perc_num(self, series_hectopunt):
        '''perc of 'a' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "a")

 
    @column_name('road_type')
    def road_type_c_perc_num(self, series_hectopunt):
        '''perc of 'c' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "c")

 
    @column_name('road_type')
    def road_type_d_perc_num(self, series_hectopunt):
        '''perc of 'd' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "d")

    @column_name('road_type')
    def road_type_x_perc_num(self, series_hectopunt):
        '''perc of 'x' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "x")

    @column_name('road_type')
    def road_type_g_perc_num(self, series_hectopunt):
        '''perc of 'g' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "g")

    @column_name('road_type')
    def road_type_y_perc_num(self, series_hectopunt):
        '''perc of 'y' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "y")
           
    @column_name('road_type')
    def road_type_f_perc_num(self, series_hectopunt):
        '''perc of 'f' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "f")

    @column_name('road_type')
    def road_type_e_perc_num(self, series_hectopunt):
        '''perc of 'e' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "e")

    @column_name('road_type')
    def road_type_h_perc_num(self, series_hectopunt):
        '''perc of 'h' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "h")

    @column_name('road_type')
    def road_type_u_perc_num(self, series_hectopunt):
        '''perc of 'u' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "u")

    @column_name('road_type')
    def road_type_q_perc_num(self, series_hectopunt):
        '''perc of 'q' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "q")

    @column_name('road_type')
    def road_type_s_perc_num(self, series_hectopunt):
        '''perc of 's' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "s")

    @column_name('road_type')
    def road_type_p_perc_num(self, series_hectopunt):
        '''perc of 'p' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "p")
     
    @column_name('road_type')
    def road_type_t_perc_num(self, series_hectopunt):
        '''perc of 't' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "t")
      
    @column_name('road_type')
    def road_type_r_perc_num(self, series_hectopunt):
        '''perc of 'r' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "r")

    @column_name('road_type')
    def road_type_v_perc_num(self, series_hectopunt):
        '''perc of 'v' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "v")
 
    @column_name('road_type')
    def road_type_j_perc_num(self, series_hectopunt):
        '''perc of 'j' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "j")

    @column_name('road_type')
    def road_type_z_perc_num(self, series_hectopunt):
        '''perc of 'z' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "z")

    @column_name('road_type')
    def road_type_w_perc_num_num(self, series_hectopunt):
        '''perc of 'w' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "w")


    @column_name('road_type')
    def road_type_k_perc_num(self, series_hectopunt):
        '''perc of 'k' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "k")

    @column_name('road_type')
    def road_type_o_perc_num(self, series_hectopunt):
        '''perc of 'o' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "o")

    @column_name('road_type')
    def road_type_i_perc_num(self, series_hectopunt):
        '''perc of 'i' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "i")

    @column_name('road_type')
    def road_type_l_perc_num(self, series_hectopunt):
        '''perc of 'l' road_type'''         
        return get_road_type_perc(self, series_hectopunt, "l")
    
    
    
    ## One would make more methods here for new features.
    ## @column_name would be name of the column the aggregation should work on
    ## name of function is the name of the feature. 
    
    

    
def generate_features(hectopunten_table, hectopunten_mapping_table, conn,
                     hectopunten_rollup_table):
    factory = HectopuntenFeatureFactory(hectopunten_table,
                                        hectopunten_mapping_table, conn,
                                       hectopunten_rollup_table)
    
    ## This is what pandas apply function can take if you want to
    ## give for each column a different aggregation method to be run.
    ## https://stackoverflow.com/questions/14529838/apply-multiple-functions-to-multiple-groupby-columns
    function_tree = {}
   
    
    
    for i in dir(factory):
        method = getattr(factory, i)
        ## Check if it's not an internal thing and is actually a method
        if not i.startswith('__') and not i.startswith('_') and hasattr(method, '__call__'):
            ## column you want to run the aggregation on.
            column_name = method.column_name
            ## if column already exists in hectopunten - its a single column feature.
            if column_name in factory.linked_hectopunten.columns:
                pass
            ## Need to create an empty column for groupby to work
            ## This is done for features which combine to hectopunten columns as one feature
            ## Eg. 'Does off-ramp exist neahttp://localhost:9999/edit/src/data/HectopuntenFeatureFactory.py#r tree' feature
            else:
                factory.linked_hectopunten.loc[:, column_name] = None
                
            feature_name = i
            try:
                function_tree[column_name][feature_name] = method
            except:
                function_tree[column_name] = {feature_name : method}
    t = factory.linked_hectopunten.groupby(['rollup_year','hectokey_merged']).agg(function_tree)
#     t = t.reset_index()
    print(t.columns)
    t.columns = t.columns.droplevel(0)
    return t.reset_index(), function_tree

def hectopunten_driver(hectopunten_table, hectopunten_mapping_table, conn,
          write_to_schema_name,
          hectopunten_rollup_table, write_frame=True):
    '''
    This is the driver function which would be the interface to making features.
    Takes the relevant information and sets up the Factory to work ahead.
    
    '''
    
    feature_frame, function_tree = generate_features(hectopunten_table=hectopunten_table,
                                                    hectopunten_mapping_table=hectopunten_mapping_table,
                                                    conn=conn,
                                                    hectopunten_rollup_table=hectopunten_rollup_table)
    ## Temporary fix to play around with the code.
    
    
    q = '''SELECT * FROM information_schema.tables WHERE table_schema = '{0}';
        '''.format(write_to_schema_name)
        
    existing_feature_tables = pd.read_sql(q, con=conn)
    if existing_feature_tables.shape[0]>0:
        existing_feature_tables = existing_feature_tables.loc[existing_feature_tables.table_name.str.contains(hectopunten_mapping_table.split('.')[-1]),:]
        if existing_feature_tables.shape[0]>0:
            existing_feature_tables['versions'] = existing_feature_tables.table_name.map(lambda x: int(x.split('_')[-1].replace('v','')))
            last_version = existing_feature_tables.versions.max()
            new_version = last_version + 1
        else:
            new_version = 1
    else:
        new_version = 1 
            
    
    write_to_table_name = "hectopunten_{0}_v{1}".format(hectopunten_mapping_table.split('.')[-1], new_version)
    
    print(write_to_table_name)
    
    if write_frame:
        pandas_to_db(feature_frame, write_path='temp_files/t2.csv',
                      schema_name=write_to_schema_name,
                      table_name=write_to_table_name, conn=conn)
        return feature_frame
    
    else:
        return feature_frame
    

    


### 
# UNCOMMENT THE DRIVER CALL TO TRY RUNNING THIS
###

# ff = driver(hectopunten_table=hectopunten_table, hectopunten_mapping_table=hectopunten_mapping_table,
#        conn=conn, write_path_temp=write_path_temp, write_to_schema_name=write_to_schema_name,
#        write_to_table_name=write_to_table_name, hectopunten_rollup_table=hectopunten_rollup_table)

