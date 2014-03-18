// let's disallow some of Javascript's more dangerous features
'use strict';

// just for convenience when importing through require
require.config({
	paths: {
	    'highcharts': '../deps/highcharts/highcharts',
		'jquery'    : '../deps/jquery/jquery'
	}
});

// `require` lets us separate our js into modules
require([
    'highcharts',
    'jquery'
], function ($) {
    console.log("loaded!");
});

