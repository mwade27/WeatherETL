CREATE TABLE cities (
    ID int NOT NULL AUTO_INCREMENT,
    Name varchar(255) NOT NULL,
    County varchar(255),
    Latitude float NOT NULL,
    Longitude float NOT NULL,
    PRIMARY KEY (ID)
)
CREATE TABLE daily_weather(
    Code varchar(50) NOT NULL,
    Date datetime NOT NULL,
    CityID int NOT NULL,
    MaxTemp float,
    MinTemp float,
    MeanTemp float,
    Precipitationsum float,
    PrecipitationHours int,
    WindSpeed float,
    DayLightDuration time,
    SunshineDuration time,
    RainSum float,
    SnowSum float,
    UVIndex float,
    Sunsettime time,
    Sunrisetime time,
    MaxWindSpeed float,
    MaxWindGust float,
    WindDirection varchar(10),
    FOREIGN KEY (CityID) REFRENCES cities(ID),
    PRIMARY KEY (Code, Date, CityID)

);