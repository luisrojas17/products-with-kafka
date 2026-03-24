package com.acme.messaging.products.rest;

import java.util.Date;

import com.acme.messaging.products.rest.dto.ErrorMessageDto;
import com.acme.messaging.products.rest.dto.ProductDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.acme.messaging.products.services.ProductService;

/**
 * This class is a REST Controller which exposes endpoints to handles the requests
 * to manage products.
 * <br/>
 * To consume the endpoints you can use next URL:
 * <br/>
 * http://localhost:&lt;port&gt;/products
 */
@Slf4j
@RestController
@RequestMapping("/products")
public class ProductController {
	
	private final ProductService productService;
	
	public ProductController(ProductService productService) {
		this.productService = productService;
	}
	
	@PostMapping
	public ResponseEntity<Object> create(@RequestBody ProductDto product) {
		
		String productId;

		try { 
			productId = productService.create(product);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new ErrorMessageDto(new Date(), e.getMessage(),"/products"));
		}
		
		return ResponseEntity.status(HttpStatus.CREATED).body(productId);
	}

}
