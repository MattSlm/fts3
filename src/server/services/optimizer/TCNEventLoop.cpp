#include <map>
#include <vector>
#include <cstdlib>
#include <cmath>
#include <ctime>

#include "Optimizer.h"
#include "common/Exceptions.h"
#include "common/Logger.h"
#include "common/panic.h"

#include "TCNEventLoop.h"

#define VarNumSamples 5

using namespace fts3::common;

namespace fts3 {
namespace optimizer {

TCNEventLoop::TCNEventLoop(OptimizerDataSource *ds,
	double convergeVariance_,
	std::time_t estTOldMinTime_,
	TCNEventPhase phase_) : 
	dataSource(ds), convergeVariance(convergeVariance_), estTOldMinTime(estTOldMinTime_), phase(phase_), pertPair(Pair("", "", ""))
{
}

void TCNEventLoop::setOptimizerDecision(ConcurrencyVector n){
	decided_n = n;
}

Pair TCNEventLoop::choosePertPair(ThroughputVector n){
	try {
		Pair p("", "", "");
		if(n.size() == 0) return p;
		auto it = n.begin();
		std::advance(it, std::rand() % n.size());
		return it->first;
	}
    catch (std::exception &e) {
		std::string stackTrace = panic::stack_dump(panic::stack_backtrace, panic::stack_backtrace_size);
		FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Stacktrace: " << stackTrace << commit;
        throw SystemError(std::string(__func__) + ": Caught exception " + e.what());
    }
    catch (...) {
        throw SystemError(std::string(__func__) + ": Caught exception ");
    }
}

/* Calculate tau for each pipe. (Number of bytes transferred since
interval start.)
See the comments to calculateTput for information about the assumptions made
in calculating these values.
TODO: remove code duplication
*/

ThroughputVector TCNEventLoop::calculateTau(int index) {
	FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Calculating tau: " << index << commit;
	try {
		ThroughputVector retval;
		if(measureInfos.size() < 2 || index == 0) { return retval; }

		TCNMeasureInfo firstMeasure = measureInfos.at(0);

		TCNMeasureInfo lastMeasure;
		if(index == -1){
			lastMeasure = measureInfos.back();
		}
		else {
			lastMeasure = measureInfos.at(index);
		}

		for(auto it = lastMeasure.bytesSentVector.begin();
			it != lastMeasure.bytesSentVector.end(); it++) {

			Pair curPair = it->first;
			double lastTransferred = it->second;
			if(firstMeasure.bytesSentVector.count(curPair) > 0) {
				// due to assumptions, the else case should never happen
				// but good to be safe anyway :)
				std::time_t intervalLength = lastMeasure.measureTime - firstMeasure.measureTime;;
				double firstTransferred = firstMeasure.bytesSentVector[curPair];
				retval[curPair] = lastTransferred - firstTransferred;
			}
		}

		return retval;
	}
    catch (std::exception &e) {
		std::string stackTrace = panic::stack_dump(panic::stack_backtrace, panic::stack_backtrace_size);
		FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Stacktrace: " << stackTrace << commit;
        throw SystemError(std::string(__func__) + ": Caught exception " + e.what());
    }
    catch (...) {
        throw SystemError(std::string(__func__) + ": Caught exception ");
    }
}


/*
Calculate throughput for each pipe.
The argument index tells us where we should calculate throughput up to.
When index == -1, then we calculate throughput up to the last measurement
interval. Other values of index are used primarily in calculateTputVariance.
Assumption: no additional files start or stop during our estimation intervals.
* No additional start: because optimizer decision is always constant.
* No files stop: because if any pipe stops being backlogged, we reset interval.
Thus, calculate throughput as follows:
* Get most recent number of bytes transferred (last measureInfo)
* Subtract number of bytes transferred at first measureInfo
*/

ThroughputVector TCNEventLoop::calculateTput(int index) {
	FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Calculating throughput: " << index << commit;
	try {
		ThroughputVector retval;
		if(measureInfos.size() < 2 || index == 0) { return retval; }
		
		TCNMeasureInfo firstMeasure;

		TCNMeasureInfo lastMeasure;
		if(index == -1){
			index = measureInfos.size() - 1;
		}

		firstMeasure = measureInfos.at(index - 1);
		lastMeasure = measureInfos.at(index);

		for(auto it = lastMeasure.bytesSentVector.begin();
			it != lastMeasure.bytesSentVector.end(); it++) {

			Pair curPair = it->first;
			double lastTransferred = it->second;
			if(firstMeasure.bytesSentVector.count(curPair) > 0) {
				// due to assumptions, the else case should never happen
				// but good to be safe anyway :)
				std::time_t intervalLength = lastMeasure.measureTime - firstMeasure.measureTime;;
				double firstTransferred = firstMeasure.bytesSentVector[curPair];
				retval[curPair] = (lastTransferred - firstTransferred)/((double)intervalLength) ;
			}
		}

		return retval;
	}
    catch (std::exception &e) {
		std::string stackTrace = panic::stack_dump(panic::stack_backtrace, panic::stack_backtrace_size);
		FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Stacktrace: " << stackTrace << commit;
        throw SystemError(std::string(__func__) + ": Caught exception " + e.what());
    }
    catch (...) {
        throw SystemError(std::string(__func__) + ": Caught exception ");
    }
}

/* Calculate throughput variance.
More specifically, calculate the maximum throughput variance over all pipes.
Note that the implementation of this is rather inefficient (calling
calculateThroughput for every index in order to store all of the throughputs
in a list, instead of calculating variance iteratively).
TODO: be clever. (Just not now.)
*/

double TCNEventLoop::calculateTputVariance() {
	FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Calculating variance: " << commit;
	try {
		if(measureInfos.size() < 3) { 
			// need 3 measurements to calculate two throughputs, which are necessary
			// if we want to have a nonzero variance
			return -1;
		}

		int numSamples = VarNumSamples * (measureInfos.size() > VarNumSamples) + 
								(measureInfos.size() - 1) * (measureInfos.size() <= VarNumSamples);

		std::vector<ThroughputVector> tputs;
		for(int i = measureInfos.size() - numSamples
			; i < measureInfos.size(); 
			i++){
			tputs.push_back(calculateTput(i));
		}
		ThroughputVector means;
		for(auto it = tputs.at(0).begin(); it != tputs.at(0).end(); it++) {
			means[it->first] = it->second;
		}
		for(int i = 1; i < tputs.size(); i++){
			ThroughputVector curTput = tputs.at(i);
			for(auto it = curTput.begin(); it != curTput.end(); it++) {
				Pair curPair = it->first;
				if(means.count(curPair) == 0){
					// shouldn't happen (due to assumption)
					// but just in case
					means[curPair] = it->second;
				}
				else {
					means[it->first] += it->second;
				}
			}
		}
		for(auto it = means.begin(); it != means.end(); it++){
			means[it->first] /= (double)(tputs.size());
		}

		ThroughputVector vars;
		for(auto it = tputs.at(0).begin(); it != tputs.at(0).end(); it++) {
			vars[it->first] = std::pow(means[it->first]-it->second, 2);
		}
		for(int i = 1; i < tputs.size(); i++){
			ThroughputVector curTput = tputs.at(i);
			for(auto it = curTput.begin(); it != curTput.end(); it++) {
				Pair curPair = it->first;
				if(vars.count(curPair) == 0){
					// really shouldn't happen
					// but just in case
					vars[curPair] = pow(means[curPair]-it->second, 2);
				}
				else {
					vars[curPair] += pow(means[curPair]-it->second, 2);
				}
			}
		}
		for(auto it = vars.begin(); it != vars.end(); it++){
			vars[it->first] /= (double)(tputs.size());
		}

		double maxVar = -1;
		for(auto it = vars.begin(); it != vars.end(); it++) {
			if(it->second > maxVar) maxVar = it->second;
		}
		FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Done calculating variance: "<< maxVar << commit;
		return std::sqrt(maxVar);
	}
    catch (std::exception &e) {
		std::string stackTrace = panic::stack_dump(panic::stack_backtrace, panic::stack_backtrace_size);
		FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Stacktrace: " << stackTrace << commit;
        throw SystemError(std::string(__func__) + ": Caught exception " + e.what());
    }
    catch (...) {
        throw SystemError(std::string(__func__) + ": Caught exception ");
    }
}

ThroughputVector addTputVecs(ThroughputVector a, ThroughputVector b){
	ThroughputVector retval;
	for(auto it = a.begin(); it != a.end(); it++){
		if(b.count(it->first) > 0){
			retval[it->first] = it->second + b[it->first];
		}
		else {
			retval[it->first] = it->second;
		}
	}
	// now take care of elements in only b
	for(auto it = b.begin(); it != b.end(); it++){
		if(a.count(it->first) == 0){
			retval[it->first] = it->second;
		}
	}
	return retval;
}

ThroughputVector mulTputVec(double c, ThroughputVector a){
	ThroughputVector retval;
	for(auto it = a.begin(); it != a.end(); it++){
		retval[it->first] = c* it->second;
	}
	return retval;
}

ThroughputVector reluTputVec(ThroughputVector a){
	ThroughputVector retval;
	for(auto it = a.begin(); it != a.end(); it++){
		retval[it->first] = (it->second < 0)?0:it->second;
	}
	return retval;
}


ThroughputVector subTputVecs(ThroughputVector a, ThroughputVector b){
	b = mulTputVec(-1, b);
	return addTputVecs(a,b);
}

double normSquaredTputVec(ThroughputVector a){
	double retval;
	for(auto it = a.begin(); it != a.end(); it++){
		retval += pow(it->second, 2);
	}
	return retval;
}

double TCNEventLoop::efficiencyFunction(ThroughputVector tau) {
	double sum = 0;
	for(auto it = tau.begin(); it != tau.end(); it++){
		sum += it->second;
	}
	return sum;
}

// lower bound
double TCNEventLoop::utilityFunction(
	ThroughputVector tau,
	ThroughputVector T,
	ThroughputVector T_target,
	double t_target,
	double dt){
	
	return efficiencyFunction(tau) - normSquaredTputVec(
		reluTputVec(subTputVecs(
			mulTputVec(t_target, T_target),
			addTputVecs(tau, mulTputVec(dt, T))
		))
	);
}

ConcurrencyVector TCNEventLoop::gradStep(){
    try {
		double t_target = ((double)estTOldMinTime)+(std::time(NULL)-qosIntervalStartTime);
		double dt = (double)estTOldMinTime;
		ThroughputVector tau = calculateTau(-1);
		ThroughputVector T_target = constructTargetTput();
		double grad = utilityFunction(tau, T_new, T_target, t_target, dt) 
			- utilityFunction(tau, T_old, T_target, t_target, dt);
		ConcurrencyVector my_n_target = n_new;
		if(my_n_target.count(pertPair) == 0){
			// SHOULD NOT HAPPEN
			// but just to be safe :)
			if(grad < 0) { my_n_target[pertPair] = 0; }
			else { my_n_target[pertPair] = grad; }
		}
		else {
			my_n_target[pertPair] += grad;
			if(my_n_target[pertPair] < 0) { my_n_target[pertPair] = 0; }
		}
		return my_n_target;
	}
    catch (std::exception &e) {
		std::string stackTrace = panic::stack_dump(panic::stack_backtrace, panic::stack_backtrace_size);
		FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Stacktrace: " << stackTrace << commit;
        throw SystemError(std::string(__func__) + ": Caught exception " + e.what());
    }
    catch (...) {
        throw SystemError(std::string(__func__) + ": Caught exception ");
    }
}

ThroughputVector TCNEventLoop::constructTargetTput(){
	ThroughputVector lowerBound;
    try {
		dataSource->getPairsLowerbound(lowerBound);
		// no lower bound for non-backlogged pipes
		for(auto it = lowerBound.begin(); it != lowerBound.end(); it++){
			if(!dataSource->isBacklogged(it->first)) { lowerBound[it->first] = 0; }
		}
	}
    catch (std::exception &e) {
		std::string stackTrace = panic::stack_dump(panic::stack_backtrace, panic::stack_backtrace_size);
		FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Stacktrace: " << stackTrace << commit;
        throw SystemError(std::string(__func__) + ": Caught exception " + e.what());
    }
    catch (...) {
        throw SystemError(std::string(__func__) + ": Caught exception ");
    }
	return lowerBound;
}

void TCNEventLoop::newQosInterval(std::time_t start) {
    try {
		phase = TCNEventPhase::estTOld;
		measureInfos.clear();
		FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "NewQoSInterval - before clearing the map" << commit;
		originInfo.clear();
		FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "NewQoSInterval - after clearing the map" << commit;
		dataSource->getOriginTransferredBytes(originInfo);
		FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "NewQoSInterval - after extracting the map" << commit;
		qosIntervalStartTime = start; 
	}
    catch (std::exception &e) {
		std::string stackTrace = panic::stack_dump(panic::stack_backtrace, panic::stack_backtrace_size);
		FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Stacktrace: " << stackTrace << commit;
        throw SystemError(std::string(__func__) + ": Caught exception " + e.what());
    }
    catch (...) {
        throw SystemError(std::string(__func__) + ": Caught exception ");
    }
}

ConcurrencyVector TCNEventLoop::step(){
	// the main state machine for the TCN optimizer

    try {
		// get active concurrency vectors
		prev_n = cur_n;
		dataSource->getActiveConcurrencyVector(cur_n);

		FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: getActiveConcurrencyVector done" << commit;

		if(cur_n.size() == 0){
			// if no active connections scheduled, then set every pipe to 1 connection
			FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: empty cur_n" << commit;
			std::list<Pair> pairs = dataSource->getActivePairs();
			// Make sure the order is always the same
			// See FTS-1094
			pairs.sort();
			for(auto it = pairs.begin(); it != pairs.end(); it++){
				cur_n[*it] = 1;
			}
			return cur_n;
		}


		// get measurements
		TCNMeasureInfo measureInfo;

		FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: before getTransferredBytesWithOrigin" << commit;
		dataSource->getTransferredBytesWithOrigin(originInfo, measureInfo.bytesSentVector, qosIntervalStartTime);
		FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: after getTransferredBytesWithOrigin" << commit;
		measureInfo.measureTime = std::time(NULL);
		measureInfos.push_back(measureInfo);
		double variance; 

		switch(phase){
		case TCNEventPhase::estTOld:
			FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: phase estTOld" << commit;
			if(cur_n != n_old) {
				// our concurrency vector is out of date.
				// either a pipe has stopped being backlogged, or we are
				// initializing.
				// either way, set our new n_old to be cur_n
				FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: estTOld, diff" << commit;
				
				// reset
				measureInfos.clear();
				epochStartTime = std::time(NULL);
				n_old = cur_n;
				setOptimizerDecision(n_old);
				break;
			}
		
			FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: estTOld, before calculateTputVariance" << commit;
			variance = calculateTputVariance();
			FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: estTOld, after calculateTputVariance" << commit;
			if(variance > 0 && variance < convergeVariance &&
				std::time(NULL)-epochStartTime > estTOldMinTime){

				// we have converged

				FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: estTOld, converged" << commit;
				T_old = calculateTput(-1);

				// perturb a new pipe
				measureInfos.clear();
				FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: estTOld, perturb a new pipe" << commit;
				do {
					pertPair = choosePertPair(T_old);
					n_new = n_old;
					n_new[pertPair] += 1;
				} while(!dataSource->isBacklogged(pertPair));
				setOptimizerDecision(n_new);
				phase = TCNEventPhase::estTNew;
			}
			break;
		case TCNEventPhase::estTNew:
			FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: phase estTNew" << commit;
			if(cur_n != n_old) {
				// our concurrency vector is out of date.
				// either a pipe has stopped being backlogged, or we are
				// initializing.
				// either way, set our new n_old to be cur_n
				
				FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: estTNew, diff" << commit;
				// reset
				measureInfos.clear();
				epochStartTime = std::time(NULL);
				n_old = cur_n;
				setOptimizerDecision(n_old);
				phase = TCNEventPhase::estTOld;
				break;
			}

			variance = calculateTputVariance();
			if(variance < convergeVariance){
				// we have converged
				FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: estTNew, converged" << commit;
				T_new = calculateTput(-1);
				// calculate gradient
				measureInfos.clear();
				FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: estTNew, before gradStep" << commit;
				n_target = gradStep();
				FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: estTNew, after gradStep" << commit;
				setOptimizerDecision(n_target);
				phase = TCNEventPhase::adjust;
				FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: estTNew, switch to adjust phase" << commit;
			}
			break;
		case TCNEventPhase::adjust:
			FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: phase adjust" << commit;
			if(prev_n != cur_n) {
				FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: adjust, diff" << commit;
				int prev_pert_n = 0;
				if(prev_n.count(pertPair) > 0) { prev_pert_n = prev_n[pertPair]; }
				int cur_pert_n = 0;
				if(cur_n.count(pertPair) > 0) { cur_pert_n = cur_n[pertPair]; }

				if(prev_pert_n > cur_pert_n) {
					// pert pipe is decreasing
					FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: adjust, decreasing" << commit;
					// if we are already below our target, then this is bad!
					if(cur_pert_n < n_target[pertPair]) {
						// cut our losses, reset to estTOld
						epochStartTime = std::time(NULL);
						measureInfos.clear();
						n_old = cur_n;
						setOptimizerDecision(n_old);
						phase = TCNEventPhase::estTOld;
						break;
					}
				}
				// update n_target
				// some other pipe might not be backlogged, but we'll just hope
				// that this doesn't affect things too much
				for(auto it = cur_n.begin(); it != cur_n.end(); it++) {
					if(!(it->first == pertPair)) {
						n_target[it->first] = it->second;
					}
				}
				setOptimizerDecision(n_target);
			}

			if(n_target == cur_n){
				//we've reached our target
				FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Time multiplexing: adjust, reach target" << commit;
				measureInfos.clear();
				n_old = n_target;
				setOptimizerDecision(n_old);
				epochStartTime = std::time(NULL);
				phase = TCNEventPhase::estTOld;
			}
			break;
		}
	}
    catch (std::exception &e) {
		std::string stackTrace = panic::stack_dump(panic::stack_backtrace, panic::stack_backtrace_size);
		FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Stacktrace: " << stackTrace << commit;
        throw SystemError(std::string(__func__) + ": Caught exception " + e.what());
    }
    catch (...) {
        throw SystemError(std::string(__func__) + ": Caught exception ");
    }
	return decided_n;
}

}
}
