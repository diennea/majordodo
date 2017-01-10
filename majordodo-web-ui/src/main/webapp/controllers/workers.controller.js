function workersController($scope, $http, $route, $timeout, $location, $state, GlobalFunctions) {
    $scope.brokerUrl = "";
    if (!$state.brokerUrl) {
        $scope.brokerUrl = $location.search().brokerUrl;
        $state.brokerUrl = $scope.brokerUrl;
    } else {
        $scope.brokerUrl = $state.brokerUrl;
    }

    $scope.status = {};
    $scope.workers = [];
    $scope.brokers = [];
    $scope.lastupdate;
    $scope.optionsUser;
    $scope.dataUser;
    $scope.optionsType;
    $scope.dataType;
    $scope.min = {};
    $scope.min.userMin;

    $scope.keyPress = function (event) {
        if (event.keyCode == 13) {
            $scope.reloadData();
        }
    }

    $scope.go = function (path) {
        $location.path(path);
    };

    $(document).on("click", ".worker-resources", function (event) {
        popResourcesTask(event.target.name);
    });
    $(document).on("click", ".worker-usages", function (event) {
        popUsagesTask(event.target.name);
    });

    $(document).on("show.bs.modal", "#resources", function () {
        $('#resources').find('.modal-dialog').css({
            'max-width': '30%',
            width: 'auto'
        });
    });

    $(document).on("show.bs.modal", "#usages", function () {
        $('#usages').find('.modal-dialog').css({
            'max-width': '80%',
            width: 'auto'
        });
        setTimeout(function () {
            window.dispatchEvent(new Event('resize'));
        }, 400);
    });

    $scope.refresh = function (wid) {
        popUsagesTask(wid);
    }

    function popResourcesTask(workerId) {
        if ($scope.brokerUrl[$scope.brokerUrl.length - 1] !== '?') {
            $scope.brokerUrl = $scope.brokerUrl + "?";
        }
        $http.get($scope.brokerUrl + "&view=resources&workerId=" + workerId).
                success(function (data, status, headers, config) {
                    $('#warning-alert').hide();
                    if (data.ok) {
                        $scope.wres = data.resources;
                        $scope.wid = workerId;
                    }
                }).
                error(function (data, status, headers, config) {
                    $('#warning-alert').fadeIn(500);
                });
    }

    function popUsagesTask(workerId) {
        if ($scope.brokerUrl[$scope.brokerUrl.length - 1] !== '?') {
            $scope.brokerUrl = $scope.brokerUrl + "?";
        }
        $http.get($scope.brokerUrl + "&view=tasks&status=running&workerId=" + workerId).
                success(function (data, status, headers, config) {
                    $('#warning-alert').hide();
                    if (data.ok) {
                        var tasks = data.tasks;
                        var map = [];
                        for (var i = 0; i < tasks.length; i++) {
                            var item = tasks[i].userId;
                            if (map[item] == null) {
                                map[item] = 0;
                            } else {
                                map[item]++;
                            }

                        }
                        var values = [];
                        if (!$scope.min.userMin) {
                            $scope.min.userMin = 1;
                        }
                        for (var item in map) {
                            if (map[item] >= $scope.min.userMin) {
                                values.push({label: item, value: map[item]});
                            }
                        }
                        $scope.optionsUser = GlobalFunctions.getDiscreteBarChartOptions("User usages", 300, "Users");
                        $scope.dataUser = [{key: "", values}];

                        var map = [];
                        for (var i = 0; i < tasks.length; i++) {
                            var item = tasks[i].tasktype;
                            if (map[item] == null) {
                                map[item] = 0;
                            } else {
                                map[item]++;
                            }

                        }
                        var values = [];
                        for (var item in map) {
                            if (map[item] > 0) {
                                values.push({label: item, value: map[item]});
                            }
                        }
                        $scope.dataType = [{key: "Type of tasks", values}];
                        $scope.optionsType = GlobalFunctions.getDiscreteBarChartOptions("Type usages", 300, "Task type");

                        $scope.wid = workerId;
                        window.dispatchEvent(new Event('resize'));
                    }
                }).
                error(function (data, status, headers, config) {
                    $('#warning-alert').fadeIn(500);
                });
    }

    $scope.reloadData = function () {
        $state.brokerUrl = $scope.brokerUrl;
if ($scope.brokerUrl[$scope.brokerUrl.length - 1] !== '?') {
            $scope.brokerUrl = $scope.brokerUrl + "?";
            
        }
        $http.get($scope.brokerUrl).
                success(function (data, status, headers, config) {
                    $('#warning-alert').hide();
                    if (data.ok) {
                        $scope.status = data.status;
                        $scope.workers = data.workers;
                        $scope.lastupdate = new Date();
                    }
                }).
                error(function (data, status, headers, config) {
                    $scope.badUrl = $scope.brokerUrl || 'URL';
                    $('#warning-alert').fadeIn(500);
                });

    };

    $scope.getWorkerColor = function (status) {
        if (status == "CONNECTED") {
            return "success";
        } else if (status == "DEAD") {
            return "danger";
        }
    }

    $(document).ready(function () {
        $('#warning-alert').hide();
        $('li').removeClass("active");
        $('#li-workers').addClass("active");
        $scope.reloadData();
    });
}
