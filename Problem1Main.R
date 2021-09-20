# LOAD NEEEDED PACKAGES, FUNCTIONS, COLOR SCHEMES ============================================================================

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

conn1 <-
  odbcDriverConnect(
    'driver=ODBC Driver 11 for SQL Server;server=server-prod18.be.local\\eifi_prod,3145;database=BentekEnergyDW;Uid=andrew_cooper; Pwd=aWll!&15lm; trusted_connection=yes'    
  )

DRILLING <- "SELECT StateName, CountyName, API, WellOrientation, SpudDate,
ReleaseDate, DrillerName, PowerType, HPDRAW, ProposedTotalDepth, SurfaceLat, SurfaceLong,
BottomHoleLat, BottomHoleLong
  FROM [BentekEnergyDW].[GasProduction].[tblEnverusRigDataPLSDB]
  WHERE StateCode IN ('ND','CO','PA','TX','NM')
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

Drilling_Data <- data.frame(API = DRILL_SPUD$Group.1, SpudDate = DRILL_SPUD$x, ReleaseDate = DRILL_RELEASE$x,
                            SurfaceLat = DRILL_SURFACE_LAT$x, SurfaceLong = DRILL_SURFACE_LONG$x, BottomLat = DRILL_BOTTOM_LAT$x,
                            BottomLong = DRILL_BOTTOM_LONG$x, Orientation = DRILL_ORIENTATION$x,
                            ProposedDepth = DRILL_PROPOSED_DEPTH$x, Power_Type = DRILL_POWER_TYPE$x, Driller_Name = DRILL_DRILLER$x, HP_Draw = DRILL_HPDRAW$x)

rm(DRILL_SPUD, DRILL_RELEASE, DRILL_SURFACE_LAT, DRILL_SURFACE_LONG, DRILL_BOTTOM_LAT, DRILL_BOTTOM_LONG, DRILL_ORIENTATION, DRILL_PROPOSED_DEPTH,
   DRILL_POWER_TYPE, DRILL_DRILLER, DRILL_HPDRAW, DRILL_STATE, DRILLING)

# LOAD SQL FRAC FOCUS DATA ============================================================================

conn2 <-
  odbcDriverConnect(
    'Driver=ODBC Driver 11 for SQL Server;server=server-dev18.be.local;database=FracFocusRegistry; Uid=andrew_cooper; Pwd=aWll!&15lm; trusted_connection=yes'
  ) 

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

FracFocus_Data$APINumber <- as.character(FracFocus_Data$APINumber)
FracFocus_Data$APINumber <- gsub("0000", "", FracFocus_Data$APINumber)
FracFocus_Data$APINumber <- gsub("0001", "", FracFocus_Data$APINumber)
FracFocus_Data$APINumber <- gsub("[^0-9.-]", "", FracFocus_Data$APINumber)
FracFocus_Data$APINumber <- as.character(FracFocus_Data$APINumber)

FracFocus_Data$JobStartDate <- as.Date(FracFocus_Data$JobStartDate)
FracFocus_Data$JobEndDate <- as.Date(FracFocus_Data$JobEndDate)

FRAC_JOB_END <- aggregate(FracFocus_Data$JobEndDate, by = list(FracFocus_Data$APINumber), FUN = max)
FRAC_JOB_START <- aggregate(FracFocus_Data$JobEndDate, by = list(FracFocus_Data$APINumber), FUN = max)
FRAC_BASE_WATER_VOLUME <- aggregate(FracFocus_Data$TotalBaseWaterVolume, by = list(FracFocus_Data$APINumber), FUN = max)
FRAC_STATE <- aggregate(FracFocus_Data$StateName, by = list(FracFocus_Data$APINumber), FUN = first)

FracFocus_Data <- data.frame(API = FRAC_STATE$Group.1, JobStart_Date = FRAC_JOB_START$x, 
                             JobEnd_Date = FRAC_JOB_END$x, BaseWater_Volume = FRAC_BASE_WATER_VOLUME$x)

rm(FRAC_FOCUS, FRAC_JOB_END, FRAC_JOB_START, FRAC_BASE_WATER_VOLUME, FRAC_STATE)

# LOAD SQL PRODUCTION DATA ============================================================================

conn4 <-
  odbcDriverConnect(
    'driver=ODBC Driver 11 for SQL Server;server=prodsql01.be.local;database=GasProductionRawData;Uid=andrew_cooper; Pwd=aIll!&15lm; trusted_connection=yes'
  )

PRODUCTION <- "SELECT PROD_DATE, LIQ, GAS, WATER, ProdType, BentekState, BentekCounty, ApiNo, Reservoir, DrillType, CurrOperName, TotalDepth, 
    FirstProdDate, Latitude, Longitude, OilGravity FROM [GasProductionRawData].[dbo].[tblPden_Prod] t1
    FULL OUTER JOIN [GasProduction].[GasProduction].[tblWells] t2 ON t1.ENTITY_ID = t2.EntityID
    WHERE BentekState IN ('ND','TX','NM','CO')
    AND FirstProdDate >= '2013-01-01'"

PRODUCTION <- sqlExecute(conn4, PRODUCTION, fetch = TRUE)

Production_Data <- PRODUCTION

Production_Data$ApiNo <- as.character(Production_Data$ApiNo)
Production_Data$ApiNo <- gsub("[^0-9.-]", "", Production_Data$ApiNo)
Production_Data$ApiNo <- gsub("0000", "", Production_Data$ApiNo)
Production_Data$ApiNo <- as.character(Production_Data$ApiNo)

Production_Data$PROD_DATE <- as.Date(Production_Data$PROD_DATE)
Production_Data$FirstProdDate <- as.Date(Production_Data$FirstProdDate)

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

Production_Data <- data.frame(API = PROD_LAT$Group.1, Lat = PROD_LAT$x, Long = PROD_LONG$x, ProdState = PROD_STATE$x,
                                      Gas_IP = PROD_GAS$x, Oil_IP = PROD_OIL$x, Water_IP = PROD_WATER$x,
                                      FirstProdDate = PROD_FIRSTPROD$x, Prod_Type = PROD_PRODTYPE$x,
                                      Drill_Type = PROD_DRILLTYPE$x, Depth = PROD_DEPTH$x, Operator = PROD_OPERATOR$x,
                                      Reservoir = PROD_RESERVOIR$x, Oil_Gravity = PROD_OIL_GRAVITY$x)

rm(PRODUCTION, PROD_LAT, PROD_LONG, PROD_GAS, PROD_OIL, PROD_WATER, PROD_FIRSTPROD, PROD_DRILLTYPE, PROD_PRODTYPE,
   PROD_DEPTH, PROD_OPERATOR, PROD_RESERVOIR, PROD_OIL_GRAVITY)

####################################################################################################

Drilling_Data$API <- as.numeric(Drilling_Data$API)
FracFocus_Data$API <- as.numeric(FracFocus_Data$API)
Production_Data$API <- as.numeric(Production_Data$API)

FINAL_DATA_JOIN <- left_join(Production_Data, Drilling_Data, by = "API")
FINAL_DATA_JOIN <- left_join(FINAL_DATA_JOIN, FracFocus_Data, by = "API")

# TARGET VARIABLE ANALYSIS ##############################################################################

# IF OIL IP HAS NULL VALUE, STATE DIDN'T REPORT DATA, SO WE NEED TO REMOVE AS A MISSING TARGET VALUE

FINAL_DATA_JOIN <- subset(FINAL_DATA_JOIN, is.na(Oil_IP) == F)
FINAL_DATA_JOIN$TargetWellPerfomance = NA

# IN THE OIL & GAS SECTOR, A "POOR" PERFORMING OIL WELL HAS INITIAL PRODUCTION OF <= 3,710 BARRELS
# IN THE OIL & GAS SECTOR, A "AVERAGE" PERFORMING OIL WELL HAS INITIAL PRODUCTION OF > 3,710 BARRELS & <= 17,435
# IN THE OIL & GAS SECTOR, A "GREAT" PERFORMING OIL WELL HAS INITIAL PRODUCTION OF > 17,435

FINAL_DATA_JOIN_A <- subset(FINAL_DATA_JOIN, Oil_IP <= 3710)
FINAL_DATA_JOIN_A$TargetWellPerfomance = as.character('Poor')

FINAL_DATA_JOIN_B <- subset(FINAL_DATA_JOIN, Oil_IP > 3710 & Oil_IP <= 17435)
FINAL_DATA_JOIN_B$TargetWellPerfomance = as.character('Average')

FINAL_DATA_JOIN_C <- subset(FINAL_DATA_JOIN, Oil_IP > 17435)
FINAL_DATA_JOIN_C$TargetWellPerfomance = as.character('Great')

FINAL_DATA_JOIN <- rbind(FINAL_DATA_JOIN_A, FINAL_DATA_JOIN_B, FINAL_DATA_JOIN_C)
rm(FINAL_DATA_JOIN_A, FINAL_DATA_JOIN_B, FINAL_DATA_JOIN_C)

summary(as.factor(FINAL_DATA_JOIN$TargetWellPerfomance))

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
describe(FINAL_DATA_JOIN$FirstProdDate)
describe(FINAL_DATA_JOIN$SpudDate)
describe(FINAL_DATA_JOIN$ReleaseDate)
describe(FINAL_DATA_JOIN$JobStart_Date)
describe(FINAL_DATA_JOIN$JobEnd_Date)

# GEOGRAPHIC VARIABLES
summary(FINAL_DATA_JOIN$ProdState)
Hmisc::describe(FINAL_DATA_JOIN$Lat)
Hmisc::describe(FINAL_DATA_JOIN$Long)
Hmisc::describe(FINAL_DATA_JOIN$SurfaceLat)
Hmisc::describe(FINAL_DATA_JOIN$SurfaceLong)
Hmisc::describe(FINAL_DATA_JOIN$BottomLat)
Hmisc::describe(FINAL_DATA_JOIN$BottomLong)

# INITIAL EDA REPORT

create_report(FINAL_DATA_JOIN,y="TargetWellPerfomance")

# FEATURE ENGINEERING ###############################################################################

# FEATURE OF INTEREST "1" - WELL LATERAL LENGTH
FINAL_DATA_JOIN$long2 <- NISTdegTOradian(FINAL_DATA_JOIN$SurfaceLong)
FINAL_DATA_JOIN$long1 <- NISTdegTOradian(FINAL_DATA_JOIN$BottomLong)
FINAL_DATA_JOIN$lat2 <- NISTdegTOradian(FINAL_DATA_JOIN$SurfaceLat)
FINAL_DATA_JOIN$lat1 <- NISTdegTOradian(FINAL_DATA_JOIN$BottomLat)
FINAL_DATA_JOIN$dlon = FINAL_DATA_JOIN$long2 - FINAL_DATA_JOIN$long1 
FINAL_DATA_JOIN$dlat = FINAL_DATA_JOIN$lat2 - FINAL_DATA_JOIN$lat1 
FINAL_DATA_JOIN$a = (sin(FINAL_DATA_JOIN$dlat/2))^2 + cos(FINAL_DATA_JOIN$lat1) * cos(FINAL_DATA_JOIN$lat2) * (sin(FINAL_DATA_JOIN$dlon/2))^2 
FINAL_DATA_JOIN$c = 2 * atan2(sqrt(FINAL_DATA_JOIN$a), sqrt(1-FINAL_DATA_JOIN$a)) 
FINAL_DATA_JOIN$Estimated_Lateral = 6371 * FINAL_DATA_JOIN$c
FINAL_DATA_JOIN$Estimated_Lateral = 3280.84 * FINAL_DATA_JOIN$Estimated_Lateral
FINAL_DATA_JOIN <- subset(FINAL_DATA_JOIN, select = -c(a,c,dlat,dlon,long2,long1,lat2,lat1))

#DRILLING_FINAL <- subset(DRILLING_FINAL, Estimated_Lateral <= 25000)
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

# FEATURE OF INTEREST "6" - FRAC JOB DAYS

FINAL_DATA_JOIN$DrilltoProdDays <- as.numeric(difftime(FINAL_DATA_JOIN$JobStart_Date , FINAL_DATA_JOIN$JobEnd_Date , units = c("days")))

# FEATURE OF INTEREST "7" - FRAC INTENSITY

for(i in 1:nrow(FINAL_DATA_JOIN)){
  FINAL_DATA_JOIN$FracIntensity[i] = FINAL_DATA_JOIN$BaseWater_Volume[i] / FINAL_DATA_JOIN$Estimated_Lateral[i]
}

# DROP OIL_IP AS CATEGORY ALREADY DEFINED 

FINAL_DATA_JOIN <- FINAL_DATA_JOIN[-c(6)]
FINAL_DATA_JOIN <- FINAL_DATA_JOIN[-c(1)]

# TEST VS SAMPLE SPLIT ###############################################################################

dt = sort(sample(nrow(FINAL_DATA_JOIN), nrow(FINAL_DATA_JOIN)*.7))
TRAIN <- FINAL_DATA_JOIN[dt,]
TEST <- FINAL_DATA_JOIN[-dt,]

# MODEL BUILD ###############################################################################

# LOGISTIC REGRESSION

MODEL_1 <- glm(TargetWellPerfomance ~ ., data = TRAIN, family = "binomial")

# SUPPORT VECTOR MACHINE (SVM)

MODEL_2 <- svm(TargetWellPerfomance~., data = TRAIN)
p1 = predict(MODEL_2, TEST)

# CLASSIFICATION & REGRESSION TREE (CART)

MODEL_3 <- rpart(TargetWellPerfomance ~., data = TRAIN, method = "class")
predicted.classes <- MODEL_3 %>% 
  predict(TEST, type = "class")
head(predicted.classes)

# RANDOM FOREST

MODEL_4 = randomForest(TargetWellPerfomance~., data=TRAIN, proximity=TRUE)

# MODEL ANALYSIS ###############################################################################

# FINAL INTERPRETATION & RESULTS ###############################################################################

# FINAL VISUALIZATIONS ###############################################################################
