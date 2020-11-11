package com.yc.clickhouse;

import com.yc.clickhouse.service.ExampleService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;

@SpringBootApplication
@Slf4j
@Controller
public class ClickHouseApplication {
	@Resource
	private ExampleService exampleService;

	public static void main(String[] args) {
		SpringApplication.run(ClickHouseApplication.class, args);
		log.info("clickHouseServer启动成功......");
	}

	@RequestMapping(value = "/test01")
	@ResponseBody
	public String index(){
		exampleService.test();
		return "hello world";
	}

}
