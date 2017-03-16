default:
	@echo "==============Finding Files=============="
	python find_files.py
	@echo "===============Finished=================="
	@echo "=========Running Psync Analysis=========="
	python psync_analysis.py
	@echo "Finished"
	@echo "=========Running GPS Analysis============"
	python gps_analysis.py
	@echo "Finished"
	@echo "============Analysing Files=============="
	python file_analysis.py
	@echo "Finished"
	@echo "============Cluster Analysis============="
	python cluster_analysis.py
	@echo "Finished"
	@echo "=============Final Analysis=============="
	python analysis.py
	@echo "Finished"
	@echo "Done - Find results in Results/ folder"

