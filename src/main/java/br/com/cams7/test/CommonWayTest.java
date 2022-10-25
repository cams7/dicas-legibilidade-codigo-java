package br.com.cams7.test;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.modelmapper.ModelMapper;

@Slf4j
public class CommonWayTest {

  private static final boolean SHOW_LOGS = true;
  private static final Map<Integer, Boolean> SHOW_TESTS =
      Map.of(1, true, 2, true, 3, true, 4, true, 5, true, 6, true, 7, true, 8, true);

  public static void main(String[] args) {
    final var app = new CommonWayTest();
    if (SHOW_TESTS.get(1)) {
      System.out.println("1. Registred order:");
      System.out.println(app.saveOrder(1l));
    }
    if (SHOW_TESTS.get(2)) {
      System.out.println("2. Registred order with invalid payment:");
      System.out.println(app.saveOrder(2l));
    }
    if (SHOW_TESTS.get(3)) {
      System.out.println("3. Customer not found:");
      System.out.println(app.saveOrder(5l));
    }
    if (SHOW_TESTS.get(4)) {
      System.out.println("4. Customer's card not found:");
      System.out.println(app.saveOrder(4l));
    }
    if (SHOW_TESTS.get(5)) {
      System.out.println("5. Customer cart's items not found:");
      try {
        System.out.println(app.saveOrder(3l));
      } catch (RuntimeException e) {
        System.out.println("5. Error: " + e.getMessage());
      }
    }
    if (SHOW_TESTS.get(6)) {
      System.out.println("6. Get all orders:");
      app.getAllOrders()
          .forEach(
              order -> {
                System.out.println(order);
              });
    }
    if (SHOW_TESTS.get(7)) {
      System.out.println("7. Get order ids:");
      System.out.println(app.getOrderIds());
    }
    if (SHOW_TESTS.get(8)) {
      System.out.println("8. Get total item products:");
      app.getTotalItemProducts()
          .forEach(
              (productId, total) -> {
                System.out.println("Product: " + productId + ", total: " + total);
              });
    }
  }

  private static final ModelMapper MODEL_MAPPER = new ModelMapper();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Map<Long, CustomerResponse> CUSTOMERS =
      List.of(
              new CustomerResponse(1l, "Gael", "Alves"),
              new CustomerResponse(2l, "Edson", "Brito"),
              new CustomerResponse(3l, "Elaine", "Teixeira"),
              new CustomerResponse(4l, "Stella", "Paz"))
          .parallelStream()
          .collect(Collectors.toMap(CustomerResponse::getId, Function.identity()));

  private static final Map<Long, CustomerCardResponse> CUSTOMER_CARDS =
      List.of(
              new CustomerCardResponse(1l, "5172563238920845"),
              new CustomerCardResponse(2l, "5585470523496195"),
              new CustomerCardResponse(3l, "4916563711189276"))
          .parallelStream()
          .collect(Collectors.toMap(CustomerCardResponse::getCustomerId, Function.identity()));

  private static final Map<Long, List<CartItemResponse>> CART_ITEMS =
      List.of(
              new CartItemResponse(1l, 101l, 1, 25.5),
              new CartItemResponse(1l, 102l, 2, 10.3),
              new CartItemResponse(1l, 103l, 3, 16.8),
              new CartItemResponse(2l, 101l, 2, 25.5),
              new CartItemResponse(2l, 102l, 5, 10.3))
          .parallelStream()
          .collect(Collectors.groupingBy(CartItemResponse::getCustomerId));

  private static final Map<Long, Boolean> CUSTOMER_PAYMENTS = Map.of(1l, true, 2l, false);

  private static final Map<String, String> ORDERS = new ConcurrentHashMap<>();

  // Webclient layer
  private Customer getCustomerById(Long customerId) {
    log("1. Get customer by id: customerId={}", customerId);
    final var response = CUSTOMERS.get(customerId);
    if (response == null) return null;
    return new Customer()
        .withCustomerId(customerId)
        .withFullName(String.format("%s %s", response.getFirstName(), response.getLastName()));
  }

  // Webclient layer
  private CustomerCard getCustomerCardByCustomerId(Long customerId) {
    log("2. Get customer's card by customer id: customerId={}", customerId);
    final var response = CUSTOMER_CARDS.get(customerId);
    if (response == null) return null;
    return MODEL_MAPPER.map(response, CustomerCard.class);
  }

  // Webclient layer
  private List<CartItem> getCartItemsByCustomerId(Long customerId) {
    log("3. Get customer cart's items by customer id: customerId={}", customerId);
    final var response = CART_ITEMS.get(customerId);
    if (CollectionUtils.isEmpty(response)) return List.of();
    return response.parallelStream()
        .map(
            item ->
                MODEL_MAPPER
                    .map(item, CartItem.class)
                    .withTotalAmount(item.getUnitPrice() * item.getQuantity()))
        .collect(Collectors.toList());
  }

  // Webclient layer
  private Boolean isValidPaymentByCustomerId(Long customerId) {
    log("5. Is valid payment by customer id: customerId={}", customerId);
    return CUSTOMER_PAYMENTS.get(customerId);
  }

  // Repository layer
  private OrderEntity saveOrder(OrderEntity order) {
    log("4. Save order: order={}", order);
    final var customer = MODEL_MAPPER.map(order.getCustomer(), CustomerModel.class);
    final var card = MODEL_MAPPER.map(order.getCard(), CustomerCardModel.class);
    final var items =
        order.getItems().parallelStream()
            .map(item -> MODEL_MAPPER.map(item, CartItemModel.class))
            .collect(Collectors.toList());
    final var model = new OrderModel();
    model.setId(UUID.randomUUID().toString());
    model.setRegistrationDate(order.getRegistrationDate().toLocalDateTime());
    model.setTotal(order.getTotalAmount());
    model.setValidPayment(order.getValidPayment());
    model.setCustomer(customer);
    model.setCard(card);
    model.setItems(items);

    try {
      ORDERS.put(model.getId(), OBJECT_MAPPER.writeValueAsString(model));
    } catch (JsonProcessingException e) {
      log.error("An error occurred while trying to save a new order", e);
      return null;
    }

    return getOrder(model);
  }

  // Repository layer
  private OrderEntity updatePaymentStatus(String orderId, Boolean validPayment) {
    log("6. Update payment status: orderId={}, validPayment={}", orderId, validPayment);

    final var json = ORDERS.get(orderId);

    if (json == null) {
      log.error("Some error happened while updating payment status on order {}", orderId);
      return null;
    }

    try {
      final var model = OBJECT_MAPPER.readValue(json, OrderModel.class);
      model.setValidPayment(validPayment);
      return getOrder(model);
    } catch (JsonProcessingException e) {
      log.error("An error occurred while trying to update payment status", e);
      return null;
    }
  }

  // Repository layer
  private List<OrderEntity> getOrders() {
    log("Get orders");
    return ORDERS.entrySet().parallelStream()
        .map(
            entry -> {
              final var orderId = entry.getKey();
              final var json = entry.getValue();

              if (json == null) {
                throw new RuntimeException(
                    String.format("Some error happened while getting order %s", orderId));
              }

              try {
                return OBJECT_MAPPER.readValue(json, OrderModel.class);
              } catch (JsonProcessingException e) {
                throw new RuntimeException("An error occurred while trying to get orders", e);
              }
            })
        .map(CommonWayTest::getOrder)
        .collect(Collectors.toList());
  }

  private static OrderEntity getOrder(OrderModel order) {
    return MODEL_MAPPER
        .map(order, OrderEntity.class)
        .withOrderId(order.getId())
        .withTotalAmount(order.getTotal())
        .withRegistrationDate(order.getRegistrationDate().atZone(ZoneId.of("America/Sao_Paulo")));
  }

  // Repository layer
  private String getIds() {
    log("Get ids");
    return getOrders().parallelStream()
        .map(OrderEntity::getOrderId)
        .distinct()
        .collect(Collectors.joining(","));
  }

  // Repository layer
  private Map<Long, Double> getTotalProducts() {
    log("Get total products");
    return getOrders().parallelStream()
        .map(OrderEntity::getItems)
        .flatMap(List::stream)
        .collect(
            Collectors.groupingBy(
                CartItem::getProductId, Collectors.summingDouble(CartItem::getTotalAmount)))
        .entrySet()
        .parallelStream()
        .sorted((e1, e2) -> compare(e1.getValue(), e2.getValue()))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
  }

  // Core layer
  public OrderEntity saveOrder(Long customerId) {
    final var customer = getCustomerById(customerId);
    if (customer == null) return null;

    final var card = getCustomerCardByCustomerId(customerId);
    if (card == null) return null;

    final var items =
        getCartItemsByCustomerId(customerId).parallelStream()
            .sorted(CommonWayTest::compare)
            .collect(Collectors.toList());

    if (CollectionUtils.isEmpty(items))
      throw new RuntimeException("There aren't items in the cart");

    var order = new OrderEntity();
    order.setRegistrationDate(ZonedDateTime.now());
    order.setTotalAmount(getTotalAmount(items));
    order.setValidPayment(false);
    order.setCustomer(customer);
    order.setCard(card);
    order.setItems(items);

    order = saveOrder(order);
    if (order == null) return null;

    final var isValidPayment = isValidPaymentByCustomerId(order.getCustomer().getCustomerId());

    order = updatePaymentStatus(order.getOrderId(), isValidPayment);

    return order;
  }

  // Core layer
  public List<OrderEntity> getAllOrders() {
    return getOrders();
  }

  // Core layer
  public String getOrderIds() {
    return getIds();
  }

  // Core layer
  public Map<Long, Double> getTotalItemProducts() {
    return getTotalProducts();
  }

  private static double getTotalAmount(List<CartItem> items) {
    return items.parallelStream().mapToDouble(CartItem::getTotalAmount).sum();
  }

  private static int compare(CartItem item1, CartItem item2) {
    return compare(item1.getTotalAmount(), item2.getTotalAmount());
  }

  private static int compare(Double totalAmount1, Double totalAmount2) {
    if (totalAmount2 > totalAmount1) return 1;
    if (totalAmount2 < totalAmount1) return -1;
    return 0;
  }

  private static void log(String message, Object... args) {
    if (SHOW_LOGS) log.info(message, args);
  }

  // Webclient layer
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CustomerResponse {
    private Long id;
    private String firstName;
    private String lastName;
  }

  // Repository layer
  @Data
  @NoArgsConstructor
  public static class CustomerModel {
    private Long customerId;
    private String fullName;
  }

  // Core layer
  @Data
  @With
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Customer {
    private Long customerId;
    private String fullName;
  }

  // Webclient layer
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CustomerCardResponse {
    private Long customerId;
    private String longNum;
  }

  // Repository layer
  @Data
  @NoArgsConstructor
  public static class CustomerCardModel {
    private String longNum;
  }

  // Core layer
  @Data
  @With
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CustomerCard {
    private String longNum;
  }

  // Webclient layer
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CartItemResponse {
    private Long customerId;
    private Long productId;
    private Integer quantity;
    private Double unitPrice;
  }

  // Repository layer
  @Data
  @NoArgsConstructor
  public static class CartItemModel {
    private Long productId;
    private Double totalAmount;
  }

  // Core layer
  @Data
  @With
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CartItem {
    private Long productId;
    private Double totalAmount;
  }

  // Repository layer
  @Data
  @NoArgsConstructor
  public static class OrderModel {
    private String id;
    private CustomerModel customer;
    private CustomerCardModel card;
    private List<CartItemModel> items;

    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private LocalDateTime registrationDate;

    private Double total;
    private Boolean validPayment;
  }

  // Core layer
  @Data
  @With
  @NoArgsConstructor
  @AllArgsConstructor
  public static class OrderEntity {
    private String orderId;
    private Customer customer;
    private CustomerCard card;
    private List<CartItem> items;
    private ZonedDateTime registrationDate;
    private Double totalAmount;
    private Boolean validPayment;
  }

  public static class LocalDateTimeSerializer extends JsonSerializer<LocalDateTime> {
    @Override
    public void serialize(LocalDateTime date, JsonGenerator generator, SerializerProvider provider)
        throws IOException {
      generator.writeString(date.toString());
    }
  }

  public static class LocalDateTimeDeserializer extends JsonDeserializer<LocalDateTime> {
    @Override
    public LocalDateTime deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      return LocalDateTime.parse(parser.getText());
    }
  }
}
