# LOAD NEEEDED PACKAGES, FUNCTIONS, COLOR SCHEMES ============================================================================
library("caret")
library("naivebayes")
library("dplyr")
library("ggplot2")
library("psych")
library("pscl")
library("leaflet.extras")
library("magrittr")
library("plumber")
library("geosphere")
library("NISTunits")
library("class")
library("fields")
library("randomcoloR")
library("svMisc")
library("timeDate")
library("RODBC")
library("dplyr")
library("ggplot2")
library("class")
library("tidyr")
library("aRpsDCA")
library("ggExtra")
library("gridExtra")
library("RODBCext")
library("MASS")
library("ggpubr")
library("grid")
library("caret")
library("fitdistrplus")
library("tseries")
library("forecast")
library("mgcv")
library("zoo")
library("xlsx")
library("EnvStats")
library("maptools")  
library("viridis")
library("leaflet")
library("aRpsDCA")
library("nnet")
library("RSNNS")
library("svMisc")
library("timeDate")
library("RODBC")
library("dplyr")
library("ggplot2")
library("class")
library("ggmap")
library("tidyr")
library("aRpsDCA")
library("ggExtra")
library("gridExtra")
library("RODBCext")
library("MASS")
library("ggpubr")
library("grid")
library("fitdistrplus")
library("tseries")
library("forecast")
library("mgcv")
library("lubridate")
library("Hmisc")
library("zoo")
library("xlsx")
library("EnvStats")
library("maptools")  
library("viridis")
library("leaflet")
library("jsonlite")
library("stringr")
library("RODBC")
library("dplyr")
library("ggplot2")
library("class")
library("ggmap")
library("tidyr")
library("aRpsDCA")
library("ggExtra")
library("gridExtra")
library("RODBCext")
library("MASS")
library("ggpubr")
library("grid")
library("plyr")
library("plotly")
library("RColorBrewer")
library("rpart")
library("rattle")
library("rpart.plot")
library("xlsx")
library("flexdashboard")
library("DT")
library("colorRamps")
library("readxl")
library("astsa")
library("shiny")
library("DataExplorer")
library("class")
library("e1071")
library("dummies")
library("randomForest")

set.seed(1234)
COLOR <- distinctColorPalette(12)

write.excel <- function(x,row.names=FALSE,col.names=TRUE,...) {
  write.table(x,"clipboard",sep="\t",row.names=row.names,col.names=col.names,...)
}

# LOAD SQL DRILLING DATA ============================================================================

# CONNECT TO LOCAL SERVER
conn1 <-
  odbcDriverConnect(
    'driver=ODBC Driver 11 for SQL Server;server=server-prod18.be.local\\eifi_prod,3145;database=BentekEnergyDW; trusted_connection=yes'    
  ) 

# SQL QUERY BRINGING IN DRILLING VARIABLES
# FILTER BASED ON STATES --> ND, CO, TX, NM
# SPUD DATES AFTER JAN 2013

DRILLING <- "SELECT StateName, CountyName, API, WellOrientation, SpudDate,
ReleaseDate, DrillerName, PowerType, HPDRAW, ProposedTotalDepth, SurfaceLat, SurfaceLong,
BottomHoleLat, BottomHoleLong
  FROM [BentekEnergyDW].[GasProduction].[tblEnverusRigDataPLSDB]
  WHERE StateCode IN ('ND','CO','TX','NM')
  AND SpudDate >= '2013-01-01'
  ORDER BY SpudDate DESC"
DRILLING <- sqlExecute(conn1, DRILLING, fetch = TRUE)
Drilling_Data <- DRILLING

# CLEAN DRILLING DATA W/ INITIAL WRANGLING ============================================================================

Drilling_Data$API <- gsub("-", "", Drilling_Data$API)
Drilling_Data$API <- gsub("[^0-9.-]", "", Drilling_Data$API)
Drilling_Data$API <- as.character(Drilling_Data$API)

Drilling_Data$SpudDate <- as.Date(Drilling_Data$SpudDate)
Drilling_Data$ReleaseDate <- as.Date(Drilling_Data$ReleaseDate)

# WE NEED TO AGGREGATE HERE, AS SOME INDIVIIDUAL WELLS HAVE MULTIPLE RECORDS
# WE ONLY NEED ONE VARIALBE PER WELL, ALSO IDENTIFIED BY THE API NUMBER
DRILL_SPUD <- aggregate(Drilling_Data$SpudDate, by = list(Drilling_Data$API), FUN = max)
DRILL_RELEASE <- aggregate(Drilling_Data$ReleaseDate, by = list(Drilling_Data$API), FUN = max)
DRILL_SURFACE_LAT <- aggregate(Drilling_Data$SurfaceLat, by = list(Drilling_Data$API), FUN = max)
DRILL_SURFACE_LONG <- aggregate(Drilling_Data$SurfaceLong, by = list(Drilling_Data$API), FUN = max)
DRILL_BOTTOM_LAT <- aggregate(Drilling_Data$BottomHoleLat, by = list(Drilling_Data$API), FUN = max)
DRILL_BOTTOM_LONG <- aggregate(Drilling_Data$BottomHoleLong, by = list(Drilling_Data$API), FUN = max)
DRILL_ORIENTATION <- aggregate(Drilling_Data$WellOrientation, by = list(Drilling_Data$API), FUN = first)
DRILL_PROPOSED_DEPTH <- aggregate(Drilling_Data$ProposedTotalDepth, by = list(Drilling_Data$API), FUN = max)
DRILL_POWER_TYPE <- aggregate(Drilling_Data$PowerType, by = list(Drilling_Data$API), FUN = first)
DRILL_DRILLER <- aggregate(Drilling_Data$DrillerName, by = list(Drilling_Data$API), FUN = first)
DRILL_HPDRAW <- aggregate(Drilling_Data$HPDRAW, by = list(Drilling_Data$API), FUN = max)
DRILL_STATE <- aggregate(Drilling_Data$StateName, by = list(Drilling_Data$API), FUN = first)

# CREATE FINAL DATAFRAME FOR DRILLED DATA
Drilling_Data <- data.frame(API = DRILL_SPUD$Group.1, SpudDate = DRILL_SPUD$x, ReleaseDate = DRILL_RELEASE$x,
                            SurfaceLat = DRILL_SURFACE_LAT$x, SurfaceLong = DRILL_SURFACE_LONG$x, BottomLat = DRILL_BOTTOM_LAT$x,
                            BottomLong = DRILL_BOTTOM_LONG$x, Orientation = DRILL_ORIENTATION$x,
                            ProposedDepth = DRILL_PROPOSED_DEPTH$x, Power_Type = DRILL_POWER_TYPE$x, Driller_Name = DRILL_DRILLER$x, HP_Draw = DRILL_HPDRAW$x)

# CLEAN UP UNWANTED DATA FRAMES, VECTORS, ETC
rm(DRILL_SPUD, DRILL_RELEASE, DRILL_SURFACE_LAT, DRILL_SURFACE_LONG, DRILL_BOTTOM_LAT, DRILL_BOTTOM_LONG, DRILL_ORIENTATION, DRILL_PROPOSED_DEPTH,
   DRILL_POWER_TYPE, DRILL_DRILLER, DRILL_HPDRAW, DRILL_STATE, DRILLING)

summary(Drilling_Data)

# LOAD SQL FRAC FOCUS DATA ============================================================================

# CONNECT TO LOCAL SERVER
conn2 <-
  odbcDriverConnect(
    'Driver=ODBC Driver 11 for SQL Server;server=server-dev18.be.local;database=FracFocusRegistry; Uid=andrew_cooper; Pwd=aWll!&15lm; trusted_connection=yes'
  ) 

# SQL QUERY BRINGING IN DRILLING VARIABLES
# FILTER BASED ON STATES --> ND, CO, TX, NM
# JOB END DATES AFTER JAN 2013

FRAC_FOCUS <- "SELECT [JobStartDate]
      ,[JobEndDate]
      ,[APINumber]
      ,[TotalBaseWaterVolume]
      ,[StateName]
  FROM [FracFocusRegistry].[dbo].[RegistryUpload]
  WHERE StateName IN ('North Dakota','ND','Texas','TX','New Mexico','NM','Colorado','CO')
  AND JobEndDate >= '2013-01-01'
  ORDER BY JobEndDate DESC"
FRAC_FOCUS <- sqlExecute(conn2, FRAC_FOCUS, fetch = TRUE)
FracFocus_Data <- FRAC_FOCUS

# CLEAN FRAC DATA W/ INITIAL WRANGLING ============================================================================

FracFocus_Data$APINumber <- as.character(FracFocus_Data$APINumber)
FracFocus_Data$APINumber <- gsub("0000", "", FracFocus_Data$APINumber)
FracFocus_Data$APINumber <- gsub("0001", "", FracFocus_Data$APINumber)
FracFocus_Data$APINumber <- gsub("[^0-9.-]", "", FracFocus_Data$APINumber)
FracFocus_Data$APINumber <- as.character(FracFocus_Data$APINumber)

FracFocus_Data$JobStartDate <- as.Date(FracFocus_Data$JobStartDate)
FracFocus_Data$JobEndDate <- as.Date(FracFocus_Data$JobEndDate)

# WE NEED TO AGGREGATE HERE, AS SOME INDIVIIDUAL WELLS HAVE MULTIPLE RECORDS
# WE ONLY NEED ONE VARIALBE PER WELL, ALSO IDENTIFIED BY THE API NUMBER
FRAC_JOB_END <- aggregate(FracFocus_Data$JobEndDate, by = list(FracFocus_Data$APINumber), FUN = max)
FRAC_JOB_START <- aggregate(FracFocus_Data$JobEndDate, by = list(FracFocus_Data$APINumber), FUN = max)
FRAC_BASE_WATER_VOLUME <- aggregate(FracFocus_Data$TotalBaseWaterVolume, by = list(FracFocus_Data$APINumber), FUN = max)
FRAC_STATE <- aggregate(FracFocus_Data$StateName, by = list(FracFocus_Data$APINumber), FUN = first)

# CREATE FINAL DATAFRAME FOR FRAC DATA
FracFocus_Data <- data.frame(API = FRAC_STATE$Group.1, JobStart_Date = FRAC_JOB_START$x, 
                             JobEnd_Date = FRAC_JOB_END$x, BaseWater_Volume = FRAC_BASE_WATER_VOLUME$x)

# CLEAN UP UNWANTED DATA FRAMES, VECTORS, ETC
rm(FRAC_FOCUS, FRAC_JOB_END, FRAC_JOB_START, FRAC_BASE_WATER_VOLUME, FRAC_STATE)

summary(FracFocus_Data)

# LOAD SQL PRODUCTION DATA ============================================================================

# CONNECT TO LOCAL SERVER
conn4 <-
  odbcDriverConnect(
    'driver=ODBC Driver 11 for SQL Server;server=prodsql01.be.local;database=GasProductionRawData;Uid=andrew_cooper; Pwd=aIll!&15lm; trusted_connection=yes'
  )

# SQL QUERY BRINGING IN DRILLING VARIABLES
# FILTER BASED ON STATES --> ND, CO, TX, NM
# FIRST PRODUCTION DATES AFTER JAN 2013

PRODUCTION <- "SELECT PROD_DATE, LIQ, GAS, WATER, ProdType, BentekState, BentekCounty, ApiNo, Reservoir, DrillType, CurrOperName, TotalDepth, 
    FirstProdDate, Latitude, Longitude, OilGravity FROM [GasProductionRawData].[dbo].[tblPden_Prod] t1
    FULL OUTER JOIN [GasProduction].[GasProduction].[tblWells] t2 ON t1.ENTITY_ID = t2.EntityID
    WHERE BentekState IN ('ND','TX','NM','CO')
    AND FirstProdDate >= '2013-01-01'"
PRODUCTION <- sqlExecute(conn4, PRODUCTION, fetch = TRUE)
Production_Data <- PRODUCTION

# CLEAN PRODUCTION DATA W/ INITIAL WRANGLING ============================================================================

Production_Data$ApiNo <- as.character(Production_Data$ApiNo)
Production_Data$ApiNo <- gsub("[^0-9.-]", "", Production_Data$ApiNo)
Production_Data$ApiNo <- gsub("0000", "", Production_Data$ApiNo)
Production_Data$ApiNo <- as.character(Production_Data$ApiNo)

Production_Data$PROD_DATE <- as.Date(Production_Data$PROD_DATE)
Production_Data$FirstProdDate <- as.Date(Production_Data$FirstProdDate)

# WE NEED TO AGGREGATE HERE, AS SOME INDIVIIDUAL WELLS HAVE MULTIPLE RECORDS
# WE ONLY NEED ONE VARIALBE PER WELL, ALSO IDENTIFIED BY THE API NUMBER
PROD_LAT <- aggregate(Production_Data$Latitude, by = list(Production_Data$ApiNo), FUN = max)
PROD_LONG <- aggregate(Production_Data$Longitude, by = list(Production_Data$ApiNo), FUN = max)
PROD_GAS <- aggregate(Production_Data$GAS, by = list(Production_Data$ApiNo), FUN = max)
PROD_OIL <- aggregate(Production_Data$LIQ, by = list(Production_Data$ApiNo), FUN = max)
PROD_WATER <- aggregate(Production_Data$WATER, by = list(Production_Data$ApiNo), FUN = max)
PROD_FIRSTPROD <- aggregate(Production_Data$FirstProdDate, by = list(Production_Data$ApiNo), FUN = max)
PROD_PRODTYPE <- aggregate(Production_Data$ProdType, by = list(Production_Data$ApiNo), FUN = first)
PROD_DRILLTYPE <- aggregate(Production_Data$DrillType, by = list(Production_Data$ApiNo), FUN = first)
PROD_DEPTH <- aggregate(Production_Data$TotalDepth, by = list(Production_Data$ApiNo), FUN = max)
PROD_OPERATOR <- aggregate(Production_Data$CurrOperName, by = list(Production_Data$ApiNo), FUN = first)
PROD_RESERVOIR <- aggregate(Production_Data$Reservoir, by = list(Production_Data$ApiNo), FUN = max)
PROD_OIL_GRAVITY <- aggregate(Production_Data$OilGravity, by = list(Production_Data$ApiNo), FUN = max)
PROD_STATE <- aggregate(Production_Data$BentekState, by = list(Production_Data$ApiNo), FUN = first)

# CREATE FINAL DATAFRAME FOR PRODUCTION DATA
Production_Data <- data.frame(API = PROD_LAT$Group.1, Lat = PROD_LAT$x, Long = PROD_LONG$x, ProdState = PROD_STATE$x,
                              Gas_IP = PROD_GAS$x, Oil_IP = PROD_OIL$x, Water_IP = PROD_WATER$x,
                              FirstProdDate = PROD_FIRSTPROD$x, Prod_Type = PROD_PRODTYPE$x,
                              Drill_Type = PROD_DRILLTYPE$x, Depth = PROD_DEPTH$x, Operator = PROD_OPERATOR$x,
                              Reservoir = PROD_RESERVOIR$x, Oil_Gravity = PROD_OIL_GRAVITY$x)

# CLEAN UP UNWANTED DATA FRAMES, VECTORS, ETC
rm(PRODUCTION, PROD_LAT, PROD_LONG, PROD_GAS, PROD_OIL, PROD_WATER, PROD_FIRSTPROD, PROD_DRILLTYPE, PROD_PRODTYPE,
   PROD_DEPTH, PROD_OPERATOR, PROD_RESERVOIR, PROD_OIL_GRAVITY, PROD_STATE)

summary(Production_Data)

####################################################################################################

# LET'S MAKE SURE THIS API NUMBER IS NUMERIC, SO THE JOIN FUNCTION WILL WORK
Drilling_Data$API <- as.numeric(Drilling_Data$API)
FracFocus_Data$API <- as.numeric(FracFocus_Data$API)
Production_Data$API <- as.numeric(Production_Data$API)

# WE WANT TO SEE HOW MANY DRILLING & FRAC RECORDS CAN BE JOINED TO THE PRODUCTION DATASET, AS WE NEED TO USE PRODUCTION VARIABLES TO DEFINE THE PREDICTION OBSERVATION OF INTEREST
FINAL_DATA_JOIN <- left_join(Production_Data, Drilling_Data, by = "API")
FINAL_DATA_JOIN <- left_join(FINAL_DATA_JOIN, FracFocus_Data, by = "API")

# FINAL STRUCTURE OF DATAFRAME
str(FINAL_DATA_JOIN)

# TARGET VARIABLE ANALYSIS ##############################################################################

# IF OIL IP HAS NULL VALUE, STATE DIDN'T REPORT DATA, SO WE NEED TO REMOVE AS A MISSING TARGET VALUE
FINAL_DATA_JOIN <- subset(FINAL_DATA_JOIN, is.na(Oil_IP) == F)
FINAL_DATA_JOIN$TargetWellPerformance = NA

# THESE DEFINITIONS WERE PROVIDED BY INDUSTRY CONTACT FOR HOW COMMODITY INVESTMENT FIRMS DETERMINE WHICH WELL HAS A POOR, AVERAGE, OR GOOD RETURN
# IN THE OIL & GAS SECTOR, A "POOR" PERFORMING OIL WELL HAS INITIAL PRODUCTION OF <= 15,000 BARRELS
# IN THE OIL & GAS SECTOR, A "GREAT" PERFORMING OIL WELL HAS INITIAL PRODUCTION OF > 15,000 BARRELS

FINAL_DATA_JOIN_A <- subset(FINAL_DATA_JOIN, Oil_IP <= 15000)
FINAL_DATA_JOIN_A$TargetWellPerformance = 0

FINAL_DATA_JOIN_B <- subset(FINAL_DATA_JOIN, Oil_IP > 15000)
FINAL_DATA_JOIN_B$TargetWellPerformance = 1

FINAL_DATA_JOIN <- rbind(FINAL_DATA_JOIN_A, FINAL_DATA_JOIN_B)
rm(FINAL_DATA_JOIN_A, FINAL_DATA_JOIN_B)

summary(as.factor(FINAL_DATA_JOIN$TargetWellPerformance))

# EXPLORATORY ANALYSIS ##############################################################################

# NUMERIC VARIABLES
Hmisc::describe(FINAL_DATA_JOIN$Water_IP)
Hmisc::describe(FINAL_DATA_JOIN$Gas_IP)
Hmisc::describe(FINAL_DATA_JOIN$Oil_Gravity)
Hmisc::describe(FINAL_DATA_JOIN$Total_Fluid)
Hmisc::describe(FINAL_DATA_JOIN$Total_Proppant)
Hmisc::describe(FINAL_DATA_JOIN$BaseWater_Volume)
Hmisc::describe(FINAL_DATA_JOIN$Depth)
Hmisc::describe(FINAL_DATA_JOIN$ProposedDepth)
Hmisc::describe(FINAL_DATA_JOIN$HP_Draw)

# CHARACTER VARIABLES
summary(as.factor(FINAL_DATA_JOIN$Prod_Type))
summary(as.factor(FINAL_DATA_JOIN$Drill_Type))
summary(as.factor(FINAL_DATA_JOIN$Operator))
summary(as.factor(FINAL_DATA_JOIN$Reservoir))

# DATE VARIABLES
summary(FINAL_DATA_JOIN$FirstProdDate)
summary(FINAL_DATA_JOIN$SpudDate)
summary(FINAL_DATA_JOIN$ReleaseDate)
summary(FINAL_DATA_JOIN$JobStart_Date)
summary(FINAL_DATA_JOIN$JobEnd_Date)

# GEOGRAPHIC VARIABLES
FLARE_MAP <- leaflet(FINAL_DATA_JOIN) %>%
  addProviderTiles(providers$OpenStreetMap, group = "Map-View") %>%
  addProviderTiles(providers$Esri.WorldImagery, group = "Satellite-View")

FLARE_MAP = addCircleMarkers(FLARE_MAP, 
                             lng = FINAL_DATA_JOIN$Long,
                             lat = FINAL_DATA_JOIN$Lat,
                             radius = 1, 
                             stroke = FALSE, 
                             fillOpacity = 1,
                             color = "red")

FLARE_MAP = addLayersControl(map = FLARE_MAP, baseGroups = c('Map-View','Satellite-View'))
FLARE_MAP = addScaleBar(FLARE_MAP)
FLARE_MAP = addMeasure(map = FLARE_MAP, position = "bottomleft")

#create_report(FINAL_DATA_JOIN,y="TargetWellPerformance")

FLARE_MAP

# FEATURE OF INTEREST "1" - WELL LATERAL LENGTH
FINAL_DATA_JOIN$long2 <- NISTdegTOradian(FINAL_DATA_JOIN$SurfaceLong)
FINAL_DATA_JOIN$long1 <- NISTdegTOradian(FINAL_DATA_JOIN$BottomLong)
FINAL_DATA_JOIN$lat2 <- NISTdegTOradian(FINAL_DATA_JOIN$SurfaceLat)
FINAL_DATA_JOIN$lat1 <- NISTdegTOradian(FINAL_DATA_JOIN$BottomLat)
FINAL_DATA_JOIN$dlon = FINAL_DATA_JOIN$long2 - FINAL_DATA_JOIN$long1 
FINAL_DATA_JOIN$dlat = FINAL_DATA_JOIN$lat2 - FINAL_DATA_JOIN$lat1 
FINAL_DATA_JOIN$a = (sin(FINAL_DATA_JOIN$dlat/2))^2 + cos(FINAL_DATA_JOIN$lat1) *
  cos(FINAL_DATA_JOIN$lat2) * (sin(FINAL_DATA_JOIN$dlon/2))^2 
FINAL_DATA_JOIN$c = 2 * atan2(sqrt(FINAL_DATA_JOIN$a), sqrt(1-FINAL_DATA_JOIN$a)) 
FINAL_DATA_JOIN$Estimated_Lateral = 6371 * FINAL_DATA_JOIN$c
FINAL_DATA_JOIN$Estimated_Lateral = 3280.84 * FINAL_DATA_JOIN$Estimated_Lateral
FINAL_DATA_JOIN <- subset(FINAL_DATA_JOIN, select = -c(a,c,dlat,dlon,long2,long1,lat2,lat1))

# FEATURE OF INTEREST "2" - WATER TO OIL (WOR) RATIO
for(i in 1:nrow(FINAL_DATA_JOIN)){
  FINAL_DATA_JOIN$WOR[i] = FINAL_DATA_JOIN$Water_IP[i] / FINAL_DATA_JOIN$Oil_IP[i]
}

# FEATURE OF INTEREST "3" - GAS TO OIL (GOR) RATIO
for(i in 1:nrow(FINAL_DATA_JOIN)){
  FINAL_DATA_JOIN$GOR[i] = FINAL_DATA_JOIN$Gas_IP[i] / FINAL_DATA_JOIN$Oil_IP[i]
}

# FEATURE OF INTEREST "4" - DRILL DAYS
FINAL_DATA_JOIN$DrillDays <- as.numeric(difftime(FINAL_DATA_JOIN$ReleaseDate , FINAL_DATA_JOIN$SpudDate , units = c("days")))

# FEATURE OF INTEREST "5" - DRILL TO PROD TIME
FINAL_DATA_JOIN$DrilltoProdDays <- as.numeric(difftime(FINAL_DATA_JOIN$FirstProdDate , FINAL_DATA_JOIN$SpudDate , units = c("days")))

# FEATURE OF INTEREST "7" - FRAC INTENSITY
for(i in 1:nrow(FINAL_DATA_JOIN)){
  FINAL_DATA_JOIN$FracIntensity[i] = FINAL_DATA_JOIN$BaseWater_Volume[i] / FINAL_DATA_JOIN$Estimated_Lateral[i]
}

# DROP OIL_IP AS CATEGORY ALREADY DEFINED, ALSO DROP DATES AS FEATURE ENGINEERING ALLOWS DATE TIME VARIABLES TO BE DROPPED FOR MODEL BUILDING
FINAL_DATA_JOIN <- FINAL_DATA_JOIN[-c(1,6,8,15,16,26,27)]

# TEST VS SAMPLE SPLIT ###############################################################################

is.na(FINAL_DATA_JOIN) <- sapply(FINAL_DATA_JOIN, is.infinite)
FINAL_DATA_JOIN <- FINAL_DATA_JOIN[!(rowSums(is.na(FINAL_DATA_JOIN))),]

FINAL_DATA_JOIN$TargetWellPerformance <- as.factor(FINAL_DATA_JOIN$TargetWellPerformance)

FINAL_DATA_JOIN$ProdState <- as.factor(FINAL_DATA_JOIN$ProdState)
FINAL_DATA_JOIN$Drill_Type <- as.factor(FINAL_DATA_JOIN$Drill_Type)
FINAL_DATA_JOIN$Power_Type <- as.factor(FINAL_DATA_JOIN$Power_Type)

dt = sort(sample(nrow(FINAL_DATA_JOIN), nrow(FINAL_DATA_JOIN)*.7))
TRAIN <- FINAL_DATA_JOIN[dt,]
TEST <- FINAL_DATA_JOIN[-dt,]

# MODEL BUILD ###############################################################################

# DIFFERENT CLASSIFICATION METHODS

MODEL_1 <- glm(TargetWellPerformance ~ Depth + ProdState + Drill_Type + Oil_Gravity
               + Power_Type + BaseWater_Volume + Estimated_Lateral +
                 WOR + GOR + DrillDays + DrilltoProdDays + FracIntensity, data = TRAIN, family = "binomial")
MODEL_2 <- rpart(TargetWellPerformance ~ Depth + ProdState + Drill_Type + Oil_Gravity
                 + Power_Type + BaseWater_Volume + Estimated_Lateral +
                   WOR + GOR + DrillDays + DrilltoProdDays + FracIntensity, data = TRAIN, method = "class")
MODEL_3 <- naive_bayes(TargetWellPerformance ~ Depth + ProdState + Drill_Type + Oil_Gravity
                       + Power_Type + BaseWater_Volume + Estimated_Lateral +
                         WOR + GOR + DrillDays + DrilltoProdDays + FracIntensity, data = TRAIN, usekernel = T) 

summary(MODEL_1)
summary(MODEL_2)
summary(MODEL_3)

FINAL_MODEL <- glm(TargetWellPerformance ~ Depth + ProdState + Oil_Gravity
                   + BaseWater_Volume + Estimated_Lateral +
                     WOR + GOR + DrillDays + DrilltoProdDays + FracIntensity, data = TRAIN, family = "binomial")

p1 = predict(MODEL_1, TEST)
p1 = as.numeric(p1)
p1_results <- ifelse(p1 > 0.5,1,0)

ACTUALS <- as.factor(TEST$TargetWellPerformance)
MODEL_1_PREDICT <- as.factor(p1_results)

#Creating confusion matrix
RESULTS_1 <- caret::confusionMatrix(MODEL_1_PREDICT, ACTUALS)
RESULTS_1

