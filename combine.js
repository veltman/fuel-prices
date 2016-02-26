var fs = require("fs"),
    csv = require("dsv")(","),
    _ = require("underscore"),
    queue = require("queue-async");

queue()
  .defer(getCSV,"data/miles-driven.csv")
  .defer(getCSV,"data/population.csv")
  .defer(getCSV,"data/gasoline-supplied-per-day.csv")
  .defer(getCSV,"data/MER_T09_04.csv")
  .defer(getCSV,"data/cpi.csv")
  .await(function(err,mileage,population,gasUsed,gasPrices,cpi){

    gasPrices = processGasPrices(gasPrices);

    //array from 1900 - 2014
    var master = population.map(function(row){
      row.population = +row.population;
      row.year = +row.year;

      return row;
    });

    mileage.forEach(function(row){
      var index = +row.Year - 1900;
      master[index].milesDriven = +row["Vehicle Miles (millions)"] * 1000000;
    });

    gasUsed.forEach(function(row){
      var index = +row.Year - 1900;
      master[index].gallonsUsed = +row["Per Day (Thousand Barrels)"] * 1000 * 365 * 42;
    });

    gasPrices.forEach(function(row){
      var index = +row.Year - 1900;
      master[index].gasPriceUnadjusted = row.gasPriceUnadjusted;
      master[index].gasPriceType = row.gasPriceType;
    })

    var cpiCurrent = +cpi[cpi.length-1].CPI;

    console.log(cpi[cpi.length-1]);

    cpi.forEach(function(row){

      var index = +row.Year - 1900;
      master[index].cpiMultiplier = cpiCurrent/+row.CPI;

    });

    master = master.filter(function(row){

      return row.milesDriven && row.gallonsUsed && row.gasPriceUnadjusted;

    }).map(function(row){

      row.milesPerCapita = row.milesDriven / row.population;
      row.fuelEfficiency = row.milesDriven / row.gallonsUsed;

      row.gasPriceAdjusted = row.gasPriceUnadjusted * row.cpiMultiplier;

      row.dollarsPerMile = row.gasPriceAdjusted / row.fuelEfficiency;

      return row;

    });

    master.sort(function(a,b){
      return a.year - b.year;
    });

    fs.writeFile("web/gas-prices.csv",csv.format(master));

  });

function processGasPrices(raw) {

  // Get only yearly prices with real data
  var filtered = raw.filter(function(row){
    return !row.Value.match(/[a-z]/i) && row.YYYYMM.slice(4) === "13";
  }).map(function(row){
    row.Value = +row.Value;
    row.Year = +row.YYYYMM.slice(0,4);
    return row;
  });

  var byYear = _.groupBy(filtered,"Year");

  // Array of one row per year, with overall price, or unleaded reg, or leaded reg
  var result = _.pairs(byYear).map(function(pair){

    var row = {
      Year: pair[0]
    };

    var byType = _.groupBy(pair[1],"MSN");

    for (key in byType) {
      if (byType[key].length !== 1) {
        console.log(byType[key]);
        throw new Error("Conflicting values in EIA data");
      }
    }

    if (byType["MGUCUUS"]) {
      row.gasPriceUnadjusted = byType["MGUCUUS"][0].Value;
      row.gasPriceType = "All grades";
    } else if (byType["RUUCUUS"]) {
      row.gasPriceUnadjusted = byType["RUUCUUS"][0].Value;
      row.gasPriceType = "Unleaded Regular";
    } else if (byType["RLUCUUS"]) {
      row.gasPriceUnadjusted = byType["RLUCUUS"][0].Value;
      row.gasPriceType = "Leaded Regular";
    }

    return row;

  });

  result.sort(function(a,b){
    return a.Year - b.Year;
  });

  return result;

}

function getCSV(file,cb) {
  fs.readFile(file,"utf8",function(err,raw){
    return cb(err,csv.parse(raw));
  });
}
