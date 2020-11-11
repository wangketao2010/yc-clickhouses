package com.yc.clickhouse.entity;
import javax.persistence.Column;
import javax.persistence.Entity;
import com.yc.clickhouse.config.annotation.ClickHousePrimaryKey;
import com.yc.clickhouse.config.annotation.ClickHouseTable;
import lombok.Data;
import java.io.Serializable;
import java.util.Date;


/**
 * 功能描述
 *
 * @author ??
 * @date 2020-11-11
 */
@Entity
@Data
@ClickHouseTable(name = "t_test")
public class TTest  extends _BaseEntity implements Serializable {

	//private static final long serialVersionUID = L;
	@Column(name = "id")
	private Integer id;
	@Column(name = "name")
	private String name;
	@Column(name = "address")
	private String address;
	@Column(name = "create_date")
	private Date createDate;
	@Column(name = "del_status")
	private Integer delStatus;
}