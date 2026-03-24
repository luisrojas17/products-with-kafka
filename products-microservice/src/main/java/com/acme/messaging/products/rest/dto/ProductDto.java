package com.acme.messaging.products.rest.dto;

import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
public class ProductDto {
	
	private String title;
	private BigDecimal price;
	private Integer quantity;

}
