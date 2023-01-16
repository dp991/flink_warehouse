package com.example.gmall_logger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController // == @Controller + @ResponseBody
@Slf4j  // 使用sl4j日志配置文件为logback.xml
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping(value = "test", method = RequestMethod.GET)
    public String test() {
        System.out.println("success");
        return "success";
    }

    @RequestMapping("applog")
    public String getLog(@RequestParam("param") String param) {
        //数据落盘
        log.info(param);
        //写入kafka
        kafkaTemplate.send("ods_base_log", param);

        return "success";
    }


}
