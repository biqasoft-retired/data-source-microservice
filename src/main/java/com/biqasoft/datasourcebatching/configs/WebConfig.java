/*
* Copyright (c) 2016 biqasoft.com




 */

package com.biqasoft.datasourcebatching.configs;

import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.*;

import java.util.List;

@Configuration
@EnableWebMvc
public class WebConfig extends WebMvcConfigurerAdapter {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**");
    }

    @Override
    public void configureContentNegotiation(ContentNegotiationConfigurer configurer) {
        configurer
                .favorPathExtension(true).
        parameterName("mediaType").
        useJaf(false).
                ignoreAcceptHeader(true).
                defaultContentType(MediaType.APPLICATION_JSON).
                mediaType("json", MediaType.APPLICATION_JSON).
                mediaType("xml", MediaType.APPLICATION_XML)
        ;
    }

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        super.configureMessageConverters(converters);
    }

    @Override
    public void configureViewResolvers(ViewResolverRegistry registry) {
        super.configureViewResolvers(registry);
    }

}
