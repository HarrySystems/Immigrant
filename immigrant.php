<?php
	/*
		Author: Luiz Guilherme de Paula Santos
		Date: 27/11/2015
		Description: Class for easy data tranfer from a database to another.
	*/

	Class Immigrant
	{
		private $lnk_from;
		private $lnk_to;
		private $fetch_buffer = 50000;
		private $query_buffer = 15;

		function __construct(
			mysqli $from,
			mysqli $to
		)
		{
			$this->lnk_from = $from;
			$this->lnk_to = $to;
		}

		// OPTIONS
			function setBuffer($value)
			{
				$this->fetch_buffer = $value;
			}

			function mirror()
			{
				$aux = clone $this;
				$aux->lnk_to = $this->lnk_from;
				$aux->lnk_from = $this->lnk_to;

				return $aux;
			}

		// READ DATABASE AND COMPARE
			function getColumns(
				mysqli $lnk,
				$table
			)
			{
				$columns = array();
				$exec = $lnk->query("DESC ".$table);

				if($exec)
				{
					while ($info = $exec->fetch_assoc())
						$columns[] = $info['Field'];
				}
				return $columns;
			}

			function getIntColumns(
				$table_from,
				$table_to
			)
			{
				$table_to = $table_to ?: $table_from;

				return array_intersect(
					$this->getColumns(
						$this->lnk_from,
						$table_from
					), 
					$this->getColumns(
						$this->lnk_to,
						$table_to
					)
				);
			}	

			function getTables(
				mysqli $lnk
			)
			{
				$tables = array();
				$exec = $lnk->query("SHOW TABLES");

				while($info = $exec->fetch_row())
					$tables[] = $info[0];

				return $tables;
			}

			function getIntTables()
			{
				return array_intersect(
					$this->getTables($this->lnk_from), 
					$this->getTables($this->lnk_to)
				);
			}

		// COPY FROM ONE TO ANOTHER
			function copyData(
				$table_from,
				$table_to = "",
				$default = array()
			)
			{
				$table_to = $table_to ?: $table_from;

				// COLUMNS
					$columns = array_values(
						$this->getIntColumns(
							$table_from,
							$table_to
						)
					);

					$default_keys = array();
					$default_values = array();
					if(!empty($default))
					{
						foreach($default as $key => $value)
						{
							foreach($columns as $column=>$column_value){
								if($key == $column_value)
								{
									unset($columns[$column]);
								}
							}

							if($value != null)
							{
								$columns[] = $key;
								$default_keys[] = $key;
								$default_values[] = $value;
							}
						}
					}

				// PARTS
					$parts = $this->lnk_from->query(
						"
							SELECT CEIL(table_rows/".$this->fetch_buffer.") parts
							FROM information_schema.tables
							WHERE table_schema = SCHEMA()
							AND table_name = '".$table_from."'
						"
					)->fetch_row()[0];

				// GET AND INSERT DATA
					for($part = 0; $part < $parts; $part++)
					{
						$exec_select = $this->lnk_from->query(
							"
								SELECT `".implode("`,`", $columns)."`
								FROM  `".$table_from."`
								LIMIT ".$this->fetch_buffer." OFFSET ".($this->fetch_buffer * $part)."
							"
						);
						trigger_error($this->lnk_from->error);

						if($exec_select)
						{
							while($info_select = $exec_select->fetch_assoc())
							{
								$this->lnk_to->query(
								// echo ("<br>".
									"
										INSERT INTO `".$table_to."`			
										(`".implode("`,`", $columns)."`)

										VALUES
										('".implode("','", $info_select)."'".
											implode(",", $default_values).")
									"
								);
							}
						}
					}
			}

			function copyTable(
				$table_from
			)
			{
				// CREATE TABLE
					$exec_create = $this->lnk_from->query(
						"
							SHOW CREATE TABLE ".$table_from."
						"
					);
					$query_create = $exec_create->fetch_row();
					$this->lnk_to->query($query_create);

				// COPY DATA
					$this->copyData(
						$table_from,
						$table_from,
						$distinct
					);
			}

			function truncate(
				$lnk,
				$table
			)
			{
				$lnk->query("TRUNCATE TABLE ".$table);

			}

			function dropTable(
				$lnk,
				$table
			)
			{
				$lnk->query("DROP TABLE ".$table);
			}

			// function to populate only difference (incremental table)

			// function to update existent rows (non-incremental table)
			// and load new data

			// function to generate table and trigger differences
			// http://stackoverflow.com/questions/1467369/invoking-a-php-script-from-a-mysql-trigger

			// X function to update structure from a table to another

			// function to run multiple queries according to the query_buffer
	}
