var modulo = angular.module('majordodo-web-ui-module', ['ngRoute', 'nvd3']);

modulo.factory('$state', function () {
    var state = {};
    state.brokerUrl;
    return state;
});

modulo.factory('GlobalFunctions', function () {
    return {
        getDiscreteBarChartOptions: function (title, height, xLabel) {
            
            return {
                chart: {
                    type: 'discreteBarChart',
                    height: height || 450,
                    margin: {
                        top: 20,
                        right: 20,
                        bottom: 50,
                        left: 55
                    },
                    x: function (d) {
                        return d.label;
                    },
                    y: function (d) {
                        return d.value;
                    },
                    showValues: true,
                    valueFormat: function (d) {
                        return d;
                    },
                    duration: 500,
                    xAxis: {
                        axisLabel: xLabel
                    },
                    yAxis: {
                        axisLabel: 'Count',
                        axisLabelDistance: -10,
                    },
                    legend: {
                        margin: {
                            top: 5,
                            right: 10,
                            bottom: 5,
                            left: 0
                        }
                    }
                }, title: {
                    enable: true,
                    text: title,
                    className: "h4",
                    css: {
                        width: "nullpx",
                        textAlign: "center"
                    }
                }

            };
        }
    };
});

modulo.config(function ($routeProvider) {
    $routeProvider.when('/home',
            {templateUrl: 'pages/home.html', controller: homeController});
    $routeProvider.when('/workers',
            {templateUrl: 'pages/workers.html', controller: workersController});
    $routeProvider.when('/search',
            {templateUrl: 'pages/search.html', controller: searchController});
    $routeProvider.when('/resources',
            {templateUrl: 'pages/resources.html', controller: resourcesController});
    $routeProvider.otherwise({redirectTo: '/home'});
});
