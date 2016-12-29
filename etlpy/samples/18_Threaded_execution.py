import sys
sys.path.append("..")
from etl import *

ActivityContainer(
    ).add(
        name      = "buh to dat", 
        activity  = FieldsPump(
            XBaseSource("G:\\Personal Data\\My Folders\\kladr\Base\\KLADR.DBF"), 
            FileTarget("KLADR.dat"))
    ).add(
        name      = "dat to csv", 
        dependsOn = ["buh to dat"],
        activity  = FieldsPump(
            FileSource("KLADR.dat"), 
            CSVTarget("KLADR.csv"))
    ).add(
        name      = "dat to dbf", 
        dependsOn = ["buh to dat"],
        activity  = FieldsPump(
            FileSource("KLADR.dat"),
            XBaseTarget("KLADR3.DBF")) 
    ).add(
        name      = "dbf to csv",
        dependsOn = ["dat to dbf"],
        activity  = FieldsPump(
            XBaseSource("KLADR3.DBF"),
            CSVTarget("KLADR3.csv"))
    ).run()
