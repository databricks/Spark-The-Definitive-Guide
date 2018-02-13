SELECT * FROM person JOIN graduateProgram
  ON person.graduate_program = graduateProgram.id


-- COMMAND ----------

SELECT * FROM person INNER JOIN graduateProgram
  ON person.graduate_program = graduateProgram.id


-- COMMAND ----------

SELECT * FROM person FULL OUTER JOIN graduateProgram
  ON graduate_program = graduateProgram.id


-- COMMAND ----------

SELECT * FROM graduateProgram LEFT OUTER JOIN person
  ON person.graduate_program = graduateProgram.id


-- COMMAND ----------

SELECT * FROM person RIGHT OUTER JOIN graduateProgram
  ON person.graduate_program = graduateProgram.id


-- COMMAND ----------

SELECT * FROM gradProgram2 LEFT SEMI JOIN person
  ON gradProgram2.id = person.graduate_program


-- COMMAND ----------

SELECT * FROM graduateProgram LEFT ANTI JOIN person
  ON graduateProgram.id = person.graduate_program


-- COMMAND ----------

SELECT * FROM graduateProgram NATURAL JOIN person


-- COMMAND ----------

SELECT * FROM graduateProgram CROSS JOIN person
  ON graduateProgram.id = person.graduate_program


-- COMMAND ----------

SELECT * FROM graduateProgram CROSS JOIN person


-- COMMAND ----------

SELECT * FROM
  (select id as personId, name, graduate_program, spark_status FROM person)
  INNER JOIN sparkStatus ON array_contains(spark_status, id)


-- COMMAND ----------

SELECT /*+ MAPJOIN(graduateProgram) */ * FROM person JOIN graduateProgram
  ON person.graduate_program = graduateProgram.id


-- COMMAND ----------

