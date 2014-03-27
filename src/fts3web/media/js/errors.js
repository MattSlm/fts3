

function ErrorsCtrl($location, $scope, pairs, Errors)
{
	// Errors
	$scope.pairs = pairs;
		
	// Filter
    $scope.showFilterDialog = function() {
    	document.getElementById('filterDialog').style.display = 'block';
    }
    
    $scope.cancelFilters = function() {
    	document.getElementById('filterDialog').style.display = 'none';
    }

	$scope.applyFilter = function() {
		var filter = $scope.filter;
		filter['source_se'] = $location.search().source_se;
		filter['dest_se'] = $location.search().dest_se;
		$location.search(filter);
		$scope.filtersModal = false;
	}
	
	$scope.filter = {
		source_se: validString($location.search().source_se),
		dest_se:   validString($location.search().dest_se),
		reason:    validString($location.search().reason)
	}
	
	// On page change, reload
	$scope.pageChanged = function(newPage) {
		$location.search('page', newPage);
	};
	
	// Set timer to trigger autorefresh
	$scope.autoRefresh = setInterval(function() {
		var filter = $location.search();
		filter.page = $scope.errors.page;
    	$scope.errors = Errors.query(filter);
	}, REFRESH_INTERVAL);
	$scope.$on('$destroy', function() {
		clearInterval($scope.autoRefresh);
	});
}


ErrorsCtrl.resolve = {
	pairs: function($rootScope, $q, $location, Errors) {
    	loading($rootScope);
    	
    	var deferred = $q.defer();

    	var page = $location.search().page;
    	if (!page || page < 1)
    		page = 1;
    	
    	Errors.query($location.search(),
  			  genericSuccessMethod(deferred, $rootScope),
			  genericFailureMethod(deferred, $rootScope, $location));
    	
    	return deferred.promise;
    }		
}



function ErrorsForPairCtrl($location, $scope, errors, ErrorsForPair)
{
	$scope.reason    = $location.search().reason;
	$scope.errors    = errors
	$scope.source_se = $location.search().source_se;
	$scope.dest_se   = $location.search().dest_se;
	
	// Filter
    $scope.showFilterDialog = function() {
    	document.getElementById('filterDialog').style.display = 'block';
    }
    
    $scope.cancelFilters = function() {
    	document.getElementById('filterDialog').style.display = 'none';
    }

	$scope.applyFilters = function() {
		$location.search($scope.filter);
		$scope.filtersModal = false;
	}

	$scope.filter = {
		source_se: validString($location.search().source_se),
		dest_se:   validString($location.search().dest_se),
		reason:    validString($location.search().reason)
	}
	
	// On page change, reload
	$scope.pageChanged = function(newPage) {
		$location.search('page', newPage);
	};
}



ErrorsForPairCtrl.resolve = {
    errors: function($rootScope, $q, $location, ErrorsForPair) {
    	loading($rootScope);
    	
    	var deferred = $q.defer();

    	var page = $location.search().page;
    	if (!page || page < 1)
    		page = 1;
    	
    	ErrorsForPair.query($location.search(),
  			  genericSuccessMethod(deferred, $rootScope),
			  genericFailureMethod(deferred, $rootScope, $location));
    	
    	return deferred.promise;
    }
}
