package com.yc.clickhouse.entity;
import javax.persistence.Column;
import javax.persistence.Entity;

import com.yc.clickhouse.config.annotation.ClickHousePrimaryKey;
import com.yc.clickhouse.config.annotation.ClickHouseTable;
import com.yc.clickhouse.generator._BaseEntity;
import lombok.Data;
import java.io.Serializable;
import java.util.Date;

@Entity
@Data
@ClickHouseTable(name = "t")
public class T extends _BaseEntity implements Serializable {

	private static final long serialVersionUID = -2260388125919493487L;
	@Column(name = "birth")
	@ClickHousePrimaryKey
	private Date birth;
	@Column(name = "id")
	private Integer id;
	@Column(name = "name")
	private String name;
	@Column(name = "point")
	private Integer point;
}