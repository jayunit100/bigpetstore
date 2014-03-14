define([], function() {

    function makeChart(chartModel, location) {
        // previously parameters:  title, indvarVals, xtitle
        var chart = new Highcharts.Chart( {
            chart : {
                renderTo : location,
                defaultSeriesType : 'bar',
                zoomType : 'xy'
            },
            title : {
                text : chartModel.title       // chart title
            },
            xAxis : {
                title : {
                    enabled : true,
                    text : chartModel.indvar.name  // x label
                },
                startOnTick : false,
                endOnTick : false,
                showLastLabel : true,
                categories : chartModel.indvar.values  // x series
            },
            yAxis : {
                title : {
                    text : 'Dependent variable'
                }
            },
            tooltip : {
                formatter : function() {
                    return chartModel.indvar.name + ": " + this.x + 
                       "<br/>" + this.series.name + ": " + this.y;
                }
            },
            legend : {
                layout : 'vertical',
                align : 'left',
                verticalAlign : 'top',
                x : 5,
                y : 5,
                floating : false,
                backgroundColor : '#FFFFFF',
                borderWidth : 1
            },
            plotOptions : {
                scatter : {
                    marker : {
                        radius : 5,
                        states : {
                            hover : {
                                enabled : true,
                                lineColor : 'rgb(100,100,100)'
                            }
                        }
                    },
                    states : {
                        hover : {
                            marker : {
                                enabled : false
                            }
                        }
                    }
                }
            }
        });
        //
        for(var i = 0; i < chartModel.depvars.length; i++) {
            var depvar = chartModel.depvars[i];
            
            var userlabel = depvar.name;
            var vals = depvar.values;
            
            chart.addSeries({
                name:  userlabel,
                data:  vals
            });
        };
        //  it's probably not necessary to return the chart
        return chart;
    }
    
    return {
        'makeChart': makeChart
    };
    
});

