function searchController($scope, $http, $route, $timeout, $location, $state, GlobalFunctions) {
    $scope.brokerUrl = "";
    if (!$state.brokerUrl) {
        $scope.brokerUrl = $location.search().brokerUrl;
        $state.brokerUrl = $scope.brokerUrl;
    } else {
        $scope.brokerUrl = $state.brokerUrl;
    }
    $scope.tasks = [];
    $scope.filters = [];
    $scope.max = '150';

    $scope.searchThis = function (id, idsearch) {
        $(document.getElementById(idsearch + "-input")).val(id);
        $scope.filters[idsearch] = id;
        $scope.reloadSearch();
    }

    $scope.keyPress = function (event) {
        if (event.keyCode == 13) {
            $scope.reloadData();
        }
    }

    $scope.go = function (path) {
        $location.path(path);
    };

    $scope.clearFilters = function () {
        $(".input-search").each(function () {
            var $div = $(this);
            $div.find(':input').val("");
            $scope.filters[$div.attr('id')] = "";
            $div.removeClass('has-success');
        });
        $scope.reloadSearch();
    };

    $scope.setTab = function (newTab) {
        $scope.tab = newTab;
        $scope.reloadData();
    };

    $scope.isSet = function (tabNum) {
        return $scope.tab === tabNum;
    };

    $scope.optionsGroup = getGroupOptions();
    $scope.optionsType = GlobalFunctions.getDiscreteBarChartOptions("Task division", null, "Task type");

    $scope.reloadData = function () {
        toogleClassSuccess();
        if ($scope.isSet('1')) {
            $scope.reloadSearch();
        } else if ($scope.isSet('2')) {
            $scope.reloadHeap();
        } else if ($scope.isSet('3')) {
            $scope.reloadSlots();
        } else {
            $scope.reloadHeap();
            $scope.reloadSearch();
        }
    }

    $scope.reloadSearch = function () {
        $state.brokerUrl = $scope.brokerUrl;
        if ($scope.brokerUrl.indexOf("?") === -1) {
            $scope.brokerUrl = $scope.brokerUrl + "?";

        }
        if (String(~~Number($scope.max)) !== $scope.max) {
            $scope.max = '150';
        }
        if ($scope.max > 300) {
            $scope.max = 300;
        }
        var theUrl = '';
        if ($scope.filters['searchTaskId']) {
            theUrl = $scope.brokerUrl + '&view=task' + '&taskId=' + encodeURIComponent($scope.filters['searchTaskId']);
            $scope.filters = [];
        } else {
            theUrl = $scope.brokerUrl + '&view=tasks';
            if ($scope.filters['searchUserId']) {
                theUrl = theUrl + '&userId=' + encodeURIComponent($scope.filters['searchUserId']);
            }
            if ($scope.filters['searchStatus']) {
                theUrl = theUrl + '&status=' + encodeURIComponent($scope.filters['searchStatus']);
            }
            if ($scope.filters['searchSlot']) {
                theUrl = theUrl + '&slot=' + encodeURIComponent($scope.filters['searchSlot']);
            }
            if ($scope.filters['searchWorkerId']) {
                theUrl = theUrl + '&workerId=' + encodeURIComponent($scope.filters['searchWorkerId']);
            }
            if ($scope.filters['searchTasktype']) {
                theUrl = theUrl + '&tasktype=' + encodeURIComponent($scope.filters['searchTasktype']);
            }
            if ($scope.max) {
                theUrl = theUrl + '&max=' + encodeURIComponent($scope.max);
            }
            $scope.filters['searchTaskId'] = "";
        }

        $http.get(theUrl).
                success(function (data, status, headers, config) {
                    $('#warning-alert').hide();
                    $scope.lastUrl = theUrl;
                    if (data.ok) {
                        $scope.tasks = data.tasks;
                        $scope.lastupdate = new Date();
                        $scope.lastCount = data.count;
                        if (!$scope.tasks) {
                            $scope.tasks = [];
                            $scope.tasks[0] = data.task;
                        }

                    }
                }).
                error(function (data, status, headers, config) {
                    $scope.badUrl = $scope.brokerUrl || 'URL';
                    $('#warning-alert').fadeIn(500);
                });

    };

    $scope.reloadHeap = function () {
        $state.brokerUrl = $scope.brokerUrl;
        if ($scope.brokerUrl.indexOf("?") === -1) {
            $scope.brokerUrl = $scope.brokerUrl + "?";

        }
        $http.get($state.brokerUrl + "&view=tasksheap").
                success(function (data, status, headers, config) {
                    $('#warning-alert').hide();
                    if (data.ok) {
                        var tasksheap = data.tasksheap.tasks;
                        var map = [];
                        for (var i = 0; i < tasksheap.length; i++) {
                            var item = tasksheap[i].group;
                            if (map[item] == null) {
                                map[item] = 0;
                            } else {
                                map[item]++;
                            }

                        }
                        var dataGroup = [];
                        for (var item in map) {
                            dataGroup.push({key: item, y: map[item]});
                        }
                        $scope.dataGroup = dataGroup;
                        var map = [];
                        for (var i = 0; i < tasksheap.length; i++) {
                            var item = tasksheap[i].taskType;
                            if (map[item] == null) {
                                map[item] = 0;
                            } else {
                                map[item]++;
                            }

                        }
                        var values = [];
                        for (var item in map) {
                            values.push({label: item, value: map[item]});
                        }
                        $scope.dataType = [{key: "Type of tasks", values}];
                        $scope.lastupdate = new Date();
                    }
                }).
                error(function (data, status, headers, config) {
                    $scope.badUrl = $scope.brokerUrl || 'URL';
                    $('#warning-alert').hide();
                    $('#warning-alert').attr("class", "alert alert-danger");
                    $('#warning-alert').fadeIn(500);
                });

    };
    $scope.reloadSlots = function () {
        $state.brokerUrl = $scope.brokerUrl;
        if ($scope.brokerUrl.indexOf("?") === -1) {
            $scope.brokerUrl = $scope.brokerUrl + "?";

        }
        $http.get($state.brokerUrl + "&view=slots").
                success(function (data, status, headers, config) {
                    $('#warning-alert').hide();
                    if (data.ok) {
                        $scope.slots = data.slots.busySlots;
                        $scope.slots.length = Object.keys($scope.slots).length;

                        $scope.lastupdate = new Date();
                    }
                }).
                error(function (data, status, headers, config) {
                    $scope.badUrl = $scope.brokerUrl || 'URL';
                    $('#warning-alert').hide();
                    $('#warning-alert').attr("class", "alert alert-danger");
                    $('#warning-alert').fadeIn(500);
                });

    };

    $scope.reloadTask = function (id) {
        popTask(id);
    }
    function popTask(taskId) {
        if ($scope.brokerUrl.indexOf("?") === -1) {
            $scope.brokerUrl = $scope.brokerUrl + "?";
        }
        var theUrl = $scope.brokerUrl + '&view=task&taskId=' + taskId;
        $http.get(theUrl).
                success(function (data, status, headers, config) {
                    if (data.ok) {
                        if (data.task.createdTimestamp && parseInt(data.task.createdTimestamp)) {
                            data.task.createdTimestamp = new Date(data.task.createdTimestamp);
                        }
                        if (data.task.deadline && parseInt(data.task.deadline)) {
                            data.task.deadline = new Date(data.task.deadline);
                        }
                        $scope.taskdetails = data.task;

                    }
                }).error(function (data, status, headers, config) {
            alert("cannot display data for task :" + taskId);
        });

    }

    $scope.getTaskColor = function (status) {
        if (status == "waiting") {
            return "warning";
        } else if (status == "error") {
            return "danger";
        } else if (status == "finished") {
            return "info";
        } else if (status == "running") {
            return "success";
        }
    }

    $(document).on("click", ".taskdetails", function (event) {
        popTask(event.target.id);
    });

    $(document).on("show.bs.modal", "#modalSearch", function () {
        $('#modalSearch').find('.modal-dialog').css({
            'max-width': '30%',
            width: 'auto'
        });
    });
    $(document).on("show.bs.modal", "#modalSlot", function () {
        $('#modalSlot').find('.modal-dialog').css({
            'max-width': '30%',
            width: 'auto'
        });
    });
    $(document).ready(function () {
        $('#warning-alert').hide();
        $('li').removeClass("active");
        $('#li-search').addClass("active");
        $scope.setTab('1');
    });

}

function toogleClassSuccess() {
    $(".input-search").each(function () {
        var $div = $(this);
        var value = $(document.getElementById($(this).attr('id') + "-input")).val();
        if (value.length === 0) {
            $div.removeClass('has-success');
        } else {
            $div.addClass('has-success');
        }
    });
}

function getGroupOptions() {
    return {
        chart: {
            type: 'pieChart',
            height: 400,

            x: function (d) {
                return d.key;
            },
            y: function (d) {
                return d.y;
            },
            showLabels: false,
            duration: 500,
            labelThreshold: 0.01,
            labelSunbeamLayout: true,
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
            text: "Task grouped by group id",
            className: "h4",
            css: {
                width: "nullpx",
                textAlign: "center"
            }
        }
    };

}
function getTypeOptions() {
    return {
        chart: {
            type: 'discreteBarChart',
            height: 450,
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
            wrapLabels: true,
            showValues: true,
            valueFormat: function (d) {
                return d;
            },
            duration: 500,
            xAxis: {
                axisLabel: 'Task type'
            },
            yAxis: {
                axisLabel: 'Count',
                axisLabelDistance: -10,
                fontSize: '100px'
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
            text: "Task grouped by type",
            className: "h4",
            css: {
                width: "nullpx",
                textAlign: "center"
            }
        }
    };
}


