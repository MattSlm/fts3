enum class TCNEventPhase = {estTOld, estTNew, adjust};
enum class TCNEventType = {measureUpdate, fileFinish};

typedef std::map<Pipe, unsigned int> ConcurrencyVector;
typedef std::map<Pipe, double> ThroughputVector;

class TCNEventLoop {
	TCNEventPhase phase = TCNEventPhase::estTOld;

	ConcurrencyVector n_old;
	Pipe pertPipe;
	ConcurrencyVector n_new;
	ConcurrencyVector n_target;

	ThroughputVector T_old;
	ThroughputVector T_new;
	ThroughputVector tau_start;

	std::vector<TCNMeasureInfo> measureInfos;

	ThroughputVector T_new;
	
	time_t epochStartTime;

	// constants

	// if throughput variance is less than this, then our estimation
	// has converged
	double convergeVariance = 1;
	time_t estTOldMinTime = 100;

	// functions
	
	void step(TCNEventType type);
}

class TCNMeasureInfo {
	std::map<Pipe, double> bytesSentVector;
	time_t measureTime;
}

void TCNEventLoop::step(TCNEventType type,
	TCNMeasureInfo measureInfo,
	Pipe fileFinishPipe){

	// this function is called by higher-level events:
	// either a file has finished transferring or a new measurement
	//    has come in.
	//
	// type tells us the type of event that called this function.
	// measureInfo is a struct providing info on the new measurement.
	// if the event was a file finish, then fileFinishPipe tells us which
	//    pipe has finished transferring.

	switch(phase){
	case TCNEventPhase::estTOld:
		if(type == TCNEventType::fileFinish){
			auto pendingTransfers = getPendingTransfers(fileFinishPipe);
			if(pendingTransfers.count < 1){
				// pipe is no longer backlogged
				// update our n_old and clear estimated throughputs
				measureInfos.clear();
				tauStart = getBytesTransferredVector();
				n_old[fileFinishPipe]-=1;
				setOptimizerDecision(n_old);
				break;
			}
			// TODO: it is assumed that a new transfer will automatically
			// be scheduled to take the place of this finished one
		}
		measureInfos.push_back(measureInfo);
		variance = calculateTputVariance(measureInfos);
		if(variance < convergeVariance &&
			time(NULL)-epochStartTime > estTOldMinTime){

			// we have converged

			T_old = calculateTput(measureInfos);

			// perturb a new pipe
			measureInfos.clear();
			do {
				pertPipe = choosePertPipe(n_old);
				n_new = n_old;
				n_new[pertPipe] += 1;
			} while(getPendingTransfers(pertPipe).count < 1);
			setOptimizerDecision(n_new);
			phase = TCNEventPhase::estTNew;
		}
		break;
	case TCNEventPhase::estTNew:
		if(type == TCNEventType::fileFinish){
			auto pendingTransfers = getPendingTransfers(fileFinishPipe);
			if(pendingTransfers.count < 1 /*n_new[fileFinishPipe]*/){
				// pipe is no longer backlogged
				// therefore reset

				measureInfos.clear();
				tauStart = getBytesTransferredVector();
				n_new[fileFinishPipe] -= 1;
				n_old = n_new;
				setOptimizerDecision(n_old);
				break;
			}
			// TODO: it is assumed that a new transfer will automatically
			// be scheduled to take the place of this finished one
		}
		measureInfos.push_back(measureInfo);
		variance = calculateTputVariance(measureInfos);
		if(variance < convergeVariance){
			// we have converged
			T_new = calculateTput(measureInfos);
			// calculate gradient
			measureInfos.clear();
			n_target = gradStep(T_old, T_new, n_old, pertPipe, tau_start, time(NULL));
			setOptimizerDecision(n_target);
			phase = TCNEventPhase::adjust;
		}
		break;
	case TCNEventPhase::adjust:
		if(type == TCNEventType::fileFinish){
			activeVector = getActiveConcurrencyVector();
			auto pendingTransfers = getPendingTransfers(fileFinishPipe);
			if(pendingTransfers.count < 1){
				// pipe is no longer backlogged

				if(fileFinishPipe != pertPipe){
					// non-backlogged pipe is not the perturbed pipe
					// hopefully this doesn't affect things too much
					// new target will take this into account
					n_target[fileFinishPipe] -= 1;
					setOptimizerDecision(n_target);
					break;
				}
				// finished pipe is our pert pipe
				// this is fine -- unless we now dip below our target
				if(activeVector[pertPipe] < n_target[pertPipe]){
					// we're supposed to be increasing but we're now decreasing
					// cut our losses, just go to estTOld
					measureInfos.clear();
					tauStart = getBytesTransferredVector();
					n_target[fileFinishPipe] -= 1;
					n_old = n_target;
					setOptimizerDecision(n_old);
					phase = TCNEventPhase::estTOld;
					break;
				}
			}
		}
		if(n_target == activeVector){
			//we've reached our target
			measureInfos.clear();
			tauStart = getBytesTransferredVector();
			n_old = n_target;
			phase = TCNEventPhase::estTOld;
		}
		break;
	}
}

int main()
{
	return 0; 
}