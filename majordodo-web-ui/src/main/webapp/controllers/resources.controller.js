function resourcesController($scope, $http, $route, $timeout, $location, $state, GlobalFunctions) {
    $scope.brokerUrl = "";
    if (!$state.brokerUrl) {
        $scope.brokerUrl = $location.search().brokerUrl;
        $state.brokerUrl = $scope.brokerUrl;
    } else {
        $scope.brokerUrl = $state.brokerUrl;
    }
    $scope.resources = [];

    $scope.go = function (path) {
        $location.path(path);
    };

    $scope.keyPress = function (event) {
        if (event.keyCode === 13) {
            $scope.reloadData();
        }
    }

    $scope.reloadData = function () {
        if ($scope.brokerUrl.indexOf("?") === -1) {
            $scope.brokerUrl = $scope.brokerUrl + "?";
        }
        $state.brokerUrl = $scope.brokerUrl;
        var theUrl = $scope.brokerUrl + '&view=resources';
        $http.get(theUrl).
                success(function (data, status, headers, config) {
                    $('#warning-alert').hide();
                    if (data.ok) {
                        var res = data.resources;
                        $scope.options = getOptions();
                        var running = [];
                        var max = [];
                        for (var i = 0; i < res.length; i++) {
                            running.push({x: res[i].id, y: res[i].runningTasks});
                            max.push({x: res[i].id, y: res[i].actualLimit});
                        }
                        var d = [{
                                values: running,
                                key: "Running tasks"
                            }, {
                                values: max,
                                key: "Global limit"
                            }];
                        $scope.data = d;
                        $scope.lastupdate = new Date();
                        $scope.resources = data.resources;
                    }
                }).
                error(function (data, status, headers, config) {
                    $scope.badUrl = $scope.brokerUrl || 'URL';
                    $('#warning-alert').fadeIn(500);
                });

    };

    $(document).ready(function () {
        $('#warning-alert').hide();
        $('li').removeClass("active");
        $('#li-resources').addClass("active");
        $scope.reloadData();
    });
}

function getOptions() {
    return {
        chart: {
            type: 'multiBarChart',
            height: 450,
            margin: {
                top: 20,
                right: 20,
                bottom: 45,
                left: 45
            },
            clipEdge: true,
            duration: 500,
            stacked: true,
            showValues: true,
            xAxis: {
                axisLabel: 'Resource',
                showMaxMin: true,
                tickFormat: function (d) {
                    return d;
                }
            },
            yAxis: {
                axisLabel: '',
                axisLabelDistance: -20,
                tickFormat: function (d) {
                    return d3.format(',f')(d);
                }
            }
        }
    };
}

