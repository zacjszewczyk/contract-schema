.PHONY: default  # Display a help message.
.PHONY: clean    # Clean ancillary files.
.PHONY: rebuild  # Rebuild a XeTeX PDF.
.PHONY: push     # Push to remote endpoints.
.PHONY: pull     # Pull from remote endpoints.

# Helper functions



default:
	@echo "Command list:"
	@echo " - default\tList commands."
	@echo " - clean\tclean all ancillary files."
	@echo " - rebuild\tRebuild a XeTeX PDF output file."
	@echo " - push\t\tPush to all remote endpoints"
	@echo " - pull\t\tPull from origin."

clean:
	-@rm main.aux main.log main.toc main.out 2> /dev/null

rebuild:
	rm main.pdf 2> /dev/null || true
	xelatex main.tex 1> /dev/null
	xelatex main.tex 1> /dev/null
	xelatex main.tex 1> /dev/null
	make clean

push:
	@git push origin --all
	@git -c http.sslverify=false push gitlab --all

pull:
	@git checkout master
	@git pull origin master
	@git -c http.sslverify=false pull gitlab master
