angular.module('ftsmon.global_filter', [])
.directive('globalFilter', function($rootScope, $parse) {
    return {
        restrict: 'E',
        scope: {onMore: '&'},
        templateUrl: STATIC_ROOT + 'html/global_filter.html',
        link: function(scope, element, attrs) {
            // First initialization
            if (typeof($rootScope.globalFilter) == 'undefined') {
                $rootScope.globalFilter = {
                    vo: '',
                    source_se: '',
                    dest_se: '',
                    time_window: 1
                };
            }
            scope.globalFilter = $rootScope.globalFilter;

            if (attrs['onMore']) {
                scope.moreFilters = function() {
                    scope.onMore();
                }
                scope.isThereMoreFilters = true;
            }
        },
        controller: function($scope, $location, Unique) {
            $scope.globalFilter = {
                vo:        validString($location.search().vo),
                source_se: validString($location.search().source_se),
                dest_se:   validString($location.search().dest_se),
                time_window: $location.search().time_window
            }

            $rootScope.globalFilter = $scope.globalFilter;

            $scope.applyGlobalFilter = function() {
                $rootScope.globalFilter = $location.search();
                $rootScope.globalFilter.vo = validString($scope.globalFilter.vo);
                $rootScope.globalFilter.source_se = validString($scope.globalFilter.source_se);
                $rootScope.globalFilter.dest_se = validString($scope.globalFilter.dest_se);
                $rootScope.globalFilter.time_window = $scope.globalFilter.time_window;
                $location.search($rootScope.globalFilter);
            }
            
            $scope.resetGlobalFilter = function() {
            	$rootScope.globalFilter = {
                        vo: '',
                        source_se: '',
                        dest_se: '',
                        time_window: 1
                };
            	$scope.globalFilter = $rootScope.globalFilter;
            	$location.search($rootScope.globalFilter);
            }

            $scope.unique = {
                vos:          Unique('vos'),
                sources:      Unique('sources'),
                destinations: Unique('destinations')
            }
        }
    };
})
.directive('applyGlobalFilter', function($rootScope) {
    return {
        restrict: 'A',
        scope: 'isolate',
        link: function(scope, element, attrs) {
            var link = element[0];
            var href = link['href'];
            link.addEventListener('click', function(event) {
                event.preventDefault();
                window.location = hrefWithFilter(href, $rootScope.globalFilter);
            });
        }
    }
})
.run(function($location, $rootScope) {
    var wrapped = $location.search.bind($location);
    $location.search = function(search, paramValue) {
        if (typeof(search) != 'string')
            mergeFilters(search, $rootScope.globalFilter);
        return wrapped(search, paramValue);
    }
});


function mergeFilters(search, globals)
{
    if (typeof(search) == 'undefined')
        return globals;
    for (key in globals) {
        if (!key in search || search[key] == null)
            search[key] = globals[key];
        else if (key in globals)
            globals[key] = search[key];
    }
}


function hrefWithFilter(href, filter)
{
    if (typeof(filter) == 'undefined')
        return href;
    
    var query = '?';
    for (key in filter) {
        var value = filter[key];
        if (value == null)
            value = '';
        else
            value = encodeURIComponent(value);
        query += key + '=' + value + '&';
    }
    
    return href + query;
}

