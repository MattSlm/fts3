
function pairState(pair)
{
	// No active with submitted is bad, and so it is
	// less than three active and more than three submitted
	if ((!pair.active && pair.submitted) || (pair.active < 3 && pair.submitted >= 3))
		return 'error';
	// Very high rate of failures, that's pretty bad
	if ((!pair.finished && pair.failed) || (pair.finished / pair.failed <= 0.8))
		return 'error';
	// Less than three actives is so-so
	if (pair.active < 3)
		return 'warning';
	// More than three active, that's good enough
	if (pair.active >= 3)
		return 'success';
	// High rate of success, that's good
	if ((pair.finished && !pair.failed) || (pair.finished / pair.failed >= 0.9))
		return 'success';
	
	// Meh
	return 'info';
}

function mergeAttrs(a, b)
{
	for (var attr in b) {
		if (typeof(b[attr]) == 'string')
			a[attr] = b[attr];
		else
			a[attr] = '';
	}
	return a;
}

function OverviewCtrl($location, $scope, overview, Overview, Unique)
{
	$scope.overview = overview;
	
	// Unique pairs and vos
	$scope.unique = Unique.all();
	
	// Paginator	
	$scope.pageMax   = 15;
	$scope.page      = $scope.overview.page;
	$scope.pageCount = $scope.overview.pageCount;

	// On page change, reload
	$scope.pageChanged = function(newPage) {
		$location.search('page', newPage);
	};
	
	// Method to choose a style for a pair
	$scope.pairState = pairState;

	// Set timer to trigger autorefresh
	$scope.autoRefresh = setInterval(function() {
		var filter = $location.search();
		filter.page = $scope.overview.page;
    	$scope.overview = Overview.query(filter);
	}, REFRESH_INTERVAL);
	$scope.$on('$destroy', function() {
		clearInterval($scope.autoRefresh);
	});
	
	// Set up filters
	$scope.filterBy = function(filter) {
		$location.search(mergeAttrs($location.search(), filter));
	}
	
	$scope.filter = {
		vo:        undefinedAsEmpty($location.search().vo),
		source_se: undefinedAsEmpty($location.search().source_se),
		dest_se:   undefinedAsEmpty($location.search().dest_se)
	}
}


OverviewCtrl.resolve = {
	overview: function($rootScope, $location, $q, Overview) {
		loading($rootScope);
		
		var deferred = $q.defer();
	
		var page = $location.search().page;
		if (!page || page < 1)
			page = 1;
		
		Overview.query($location.search(), function(data) {
			deferred.resolve(data);
			stopLoading($rootScope);
		});
		
		return deferred.promise;
	}
}
