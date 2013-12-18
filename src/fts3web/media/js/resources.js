angular.module('ftsmon.resources', ['ngResource'])
.factory('Job', function($resource) {
	return $resource('jobs/:jobId', {}, {
		query: {method: 'GET',
			    isArray: false},
	})
})
.factory('Files', function($resource) {
	return $resource('jobs/:jobId/files', {}, {
		query: {method: 'GET',
			    isArray: false},
	})
})
.factory('ArchivedJobs', function($resource) {
	return $resource('archive', {}, {
		query: {method: 'GET',
			    isArray: false},
	})
})
.factory('Transfers', function($resource) {
	return $resource('transfers', {}, {
		query: {method: 'GET', isArray: false}
	})
})
.factory('Overview', function($resource) {
	return $resource('overview', {}, {
		query: {method: 'GET', isArray: false}
	})
})
.factory('Optimizer', function($resource) {
	return $resource('optimizer', {}, {
		query: {method: 'GET', isArray: false}
	})
})
.factory('OptimizerDetailed', function($resource) {
	return $resource('optimizer/detailed', {}, {
		query: {method: 'GET', isArray: false}
	})
})
.factory('Errors', function($resource) {
	return $resource('errors', {}, {
		query: {method: 'GET', isArray: false}
	})
})
.factory('FilesWithError', function($resource) {
	return $resource('errors/list', {}, {
		query: {method: 'GET', isArray: false}
	})
})
.factory('ConfigAudit', function($resource) {
	return $resource('config/audit', {}, {
		query: {method: 'GET', isArray: false}
	})
})
.factory('Statistics', function($resource) {
	return $resource('stats/', {}, {
		query: {method: 'GET', isArray: false}
	})
})
.factory('Servers', function($resource) {
	return $resource('stats/servers', {}, {
		query: {method: 'GET', isArray: false}
	})
})
.factory('StatsVO', function($resource) {
	return $resource('stats/vo', {}, {
		query: {method: 'GET', isArray: false}
	})
})
.factory('Profile', function($resource) {
	return $resource('stats/profiling', {}, {
		query: {method: 'GET', isArray: false}
	})
})
.factory('SlowQueries', function($resource) {
	return $resource('stats/slowqueries', {}, {
		query: {method: 'GET', isArray: false}
	})
})
.factory('Unique', function($q, $http) {
	return function(field) {
		var deferred = $q.defer();
		$http({method: 'GET', url: 'unique/' + field})
			.success(function(data) {
				deferred.resolve(data);
			})
			.error(function(data) {
				deferred.resolve([]);
			});
		return deferred.promise;
	}
})
.factory('ConfigServer', function($resource) {
	return $resource('config/server', {}, {
		all: {method: 'GET', isArray: false}
	})
})
.factory('ConfigLinks', function($resource) {
	return $resource('config/links', {}, {
		query: {method: 'GET', isArray: false}
	})
})
;
