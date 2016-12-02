VSQL=/opt/vertica/bin/vsql
VUSER=vertica
RUNVSQL=$(VSQL) -U $(VUSER)

help:
	@echo targets
	@echo - drop
	@echo - create
	@echo - load
	@echo - truncate
	@echo - vsql

drop:
	@$(RUNVSQL) -c '\i ddl-drop.sql'

create:
	@$(RUNVSQL) -c '\i ddl-create.sql'

load:
	@$(RUNVSQL) -c '\i ddl-load.sql'

truncate:
	@$(RUNVSQL) -c '\i ddl-truncate.sql'

vsql:
	@$(RUNVSQL)
